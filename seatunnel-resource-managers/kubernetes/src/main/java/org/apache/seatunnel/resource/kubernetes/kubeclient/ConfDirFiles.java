/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.resource.kubernetes.kubeclient;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Loads first-level configuration files from {@code kubernetes.conf-dir} for packaging into a
 * ConfigMap.
 *
 * <p>Filtering mirrors Spark {@code KubernetesClientUtils.listConfFiles}: skip archives ({@code
 * gz|zip|jar|tar}), skip {@code *.template}, skip non-UTF-8 with a warning, keep every other
 * first-level regular file (including yaml/properties such as {@code seatunnel.yaml}).
 *
 * <p>Size rule: each ConfigMap entry costs {@code UTF-8(key) + UTF-8(value)} bytes. Spark 3.5 uses
 * {@code String.length()} (UTF-16 code units) and may silently skip files that do not fit;
 * SeaTunnel uses UTF-8 byte length (closer to the etcd / ConfigMap object limit) and <strong>fails
 * submission</strong> when the packaged set would exceed {@code kubernetes.configmap.max-size}, so
 * a half-hidden directory mount cannot ship.
 */
@Slf4j
public final class ConfDirFiles {

    private static final Pattern BINARY_OR_ARCHIVE =
            Pattern.compile(".*\\.(gz|zip|jar|tar)", Pattern.CASE_INSENSITIVE);
    private static final Pattern TEMPLATE =
            Pattern.compile(".*\\.template", Pattern.CASE_INSENSITIVE);

    private ConfDirFiles() {}

    /**
     * Lists and loads eligible conf-dir files that fit under {@code maxSize} bytes of ConfigMap
     * payload.
     *
     * @param confDir local directory whose first-level regular files are considered
     * @param maxSize maximum accumulated UTF-8 payload ({@code key bytes + value bytes})
     * @return ordered map of file name to UTF-8 content
     * @throws IllegalArgumentException if {@code confDir} is invalid or the payload exceeds {@code
     *     maxSize}
     * @throws IOException if the directory cannot be listed
     */
    public static Map<String, String> load(Path confDir, long maxSize) throws IOException {
        if (confDir == null || !Files.isDirectory(confDir)) {
            throw new IllegalArgumentException(
                    "kubernetes.conf-dir must be an existing directory: " + confDir);
        }
        List<Path> candidates = listConfFiles(confDir);
        candidates.sort(
                Comparator.comparingLong(
                                (Path file) -> {
                                    try {
                                        return utf8Size(file.getFileName().toString())
                                                + Files.size(file);
                                    } catch (IOException e) {
                                        throw new IllegalStateException(e);
                                    }
                                })
                        .thenComparing(file -> file.getFileName().toString()));
        Map<String, String> included = new LinkedHashMap<>();
        long used = 0L;
        for (Path file : candidates) {
            String name = file.getFileName().toString();
            String content;
            try {
                content = readUtf8(file);
            } catch (CharacterCodingException failure) {
                log.warn(
                        "Unable to read a non UTF-8 encoded file {}. Skipping...",
                        file.toAbsolutePath(),
                        failure);
                continue;
            }
            long cost = utf8Size(name) + utf8Size(content);
            if (used + cost > maxSize) {
                throw new IllegalArgumentException(
                        "kubernetes.conf-dir payload exceeds kubernetes.configmap.max-size ("
                                + maxSize
                                + " bytes) while adding '"
                                + name
                                + "' (entry cost "
                                + cost
                                + ", already used "
                                + used
                                + "). Reduce files or raise the limit (max "
                                + KubernetesConstants.CONFIG_MAP_MAX_SIZE_LIMIT
                                + "). SeaTunnel fails fast instead of silently omitting files that "
                                + "would leave a half-hidden config mount.");
            }
            included.put(name, content);
            used += cost;
        }
        if (!included.isEmpty()) {
            log.info(
                    "SeaTunnel configuration files loaded from {} : {}",
                    confDir.toAbsolutePath(),
                    String.join(",", included.keySet()));
        }
        return included;
    }

    /**
     * First-level regular files that are not archives or templates. Oversized files are kept so
     * {@link #load(Path, long)} can fail fast rather than silently drop them.
     */
    static List<Path> listConfFiles(Path confDir) throws IOException {
        List<Path> files = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(confDir)) {
            for (Path path : stream) {
                if (!Files.isRegularFile(path)) {
                    continue;
                }
                String name = path.getFileName().toString();
                if (BINARY_OR_ARCHIVE.matcher(name).matches() || TEMPLATE.matcher(name).matches()) {
                    continue;
                }
                files.add(path);
            }
        }
        return files;
    }

    /** Builds a DNS-label-safe ConfigMap name {@code <appId>-conf-map}. */
    public static String configMapName(String applicationId) {
        String suffix = KubernetesConstants.CONF_DIR_CONFIG_MAP_SUFFIX;
        int maxPrefix = KubernetesConstants.DNS_LABEL_MAX_LENGTH - suffix.length();
        String prefix = applicationId.toLowerCase(Locale.ROOT);
        if (prefix.length() > maxPrefix) {
            prefix = prefix.substring(0, maxPrefix).replaceAll("-+$", "");
        }
        if (prefix.isEmpty()) {
            prefix = "seatunnel";
        }
        return prefix + suffix;
    }

    /** UTF-8 byte length of a ConfigMap key or value string. */
    static long utf8Size(String text) {
        return text.getBytes(StandardCharsets.UTF_8).length;
    }

    private static String readUtf8(Path file) throws IOException {
        CharsetDecoder decoder =
                StandardCharsets.UTF_8
                        .newDecoder()
                        .onMalformedInput(CodingErrorAction.REPORT)
                        .onUnmappableCharacter(CodingErrorAction.REPORT);
        return decoder.decode(ByteBuffer.wrap(Files.readAllBytes(file))).toString();
    }
}
