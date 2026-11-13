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
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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
 * submission</strong> when the packaged set would exceed {@code kubernetes.conf-dir.max-size}, so a
 * half-hidden directory mount cannot ship.
 *
 * <p>ConfigMap data keys must match {@code [-._a-zA-Z0-9]+}; file names that do not are rejected
 * before the API call.
 */
@Slf4j
public final class ConfDirFiles {

    private static final Pattern BINARY_OR_ARCHIVE =
            Pattern.compile(".*\\.(gz|zip|jar|tar)", Pattern.CASE_INSENSITIVE);
    private static final Pattern TEMPLATE =
            Pattern.compile(".*\\.template", Pattern.CASE_INSENSITIVE);
    /** Kubernetes ConfigMap data key regex. */
    public static final Pattern CONFIG_MAP_KEY_PATTERN = Pattern.compile("[-._a-zA-Z0-9]+");

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
        ListedConfFiles listed = listConfFilesWithSkips(confDir);
        if (!listed.skipped.isEmpty()) {
            log.info(
                    "kubernetes.conf-dir {}: skipped {} first-level entr{}: {}",
                    confDir.toAbsolutePath(),
                    listed.skipped.size(),
                    listed.skipped.size() == 1 ? "y" : "ies",
                    listed.skipped.stream()
                            .map(skip -> skip.name + " (" + skip.reason + ")")
                            .collect(Collectors.joining(", ")));
        }
        List<Path> candidates = new ArrayList<>(listed.files);
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
            if (!CONFIG_MAP_KEY_PATTERN.matcher(name).matches()) {
                throw new IllegalArgumentException(
                        "kubernetes.conf-dir file '"
                                + name
                                + "' is not a valid ConfigMap data key (must match [-._a-zA-Z0-9]+)."
                                + " Rename the file under "
                                + confDir.toAbsolutePath()
                                + ".");
            }
            String content;
            try {
                content = readUtf8(file);
            } catch (CharacterCodingException failure) {
                log.warn(
                        "Unable to read a non UTF-8 encoded file {}. Skipping...",
                        file.toAbsolutePath());
                continue;
            }
            long cost = utf8Size(name) + utf8Size(content);
            if (used + cost > maxSize) {
                long projectedTotal = used + cost;
                throw oversized(confDir, maxSize, name, cost, used, projectedTotal, included);
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

    private static IllegalArgumentException oversized(
            Path confDir,
            long maxSize,
            String failingName,
            long failingCost,
            long alreadyUsed,
            long projectedTotal,
            Map<String, String> included) {
        List<Map.Entry<String, Long>> sizes = new ArrayList<>();
        for (Map.Entry<String, String> entry : included.entrySet()) {
            sizes.add(
                    new AbstractMap.SimpleImmutableEntry<>(
                            entry.getKey(), utf8Size(entry.getKey()) + utf8Size(entry.getValue())));
        }
        sizes.add(new AbstractMap.SimpleImmutableEntry<>(failingName, failingCost));
        sizes.sort((left, right) -> Long.compare(right.getValue(), left.getValue()));
        String largest =
                sizes.stream()
                        .limit(5)
                        .map(entry -> entry.getKey() + "=" + entry.getValue() + "B")
                        .collect(Collectors.joining(", "));
        String advice;
        if (maxSize >= KubernetesConstants.CONFIG_MAP_MAX_SIZE_LIMIT) {
            advice =
                    "The limit is already the Kubernetes ConfigMap hard cap ("
                            + KubernetesConstants.CONFIG_MAP_MAX_SIZE_LIMIT
                            + "). Move large files out of kubernetes.conf-dir and provide them via "
                            + "kubernetes.configmap.<entry>.(ref|data), a Secret, or the container image.";
        } else {
            advice =
                    "Reduce files under "
                            + confDir.toAbsolutePath()
                            + " or raise "
                            + "kubernetes.conf-dir.max-size (hard cap "
                            + KubernetesConstants.CONFIG_MAP_MAX_SIZE_LIMIT
                            + ").";
        }
        return new IllegalArgumentException(
                "kubernetes.conf-dir payload total "
                        + projectedTotal
                        + " bytes exceeds kubernetes.conf-dir.max-size="
                        + maxSize
                        + " (hard cap "
                        + KubernetesConstants.CONFIG_MAP_MAX_SIZE_LIMIT
                        + ") while adding '"
                        + failingName
                        + "' (entry cost "
                        + failingCost
                        + ", already used "
                        + alreadyUsed
                        + "). Largest entries: "
                        + largest
                        + ". "
                        + advice);
    }

    /**
     * First-level regular files that are not archives or templates. Oversized files are kept so
     * {@link #load(Path, long)} can fail fast rather than silently drop them.
     */
    static List<Path> listConfFiles(Path confDir) throws IOException {
        return listConfFilesWithSkips(confDir).files;
    }

    static ListedConfFiles listConfFilesWithSkips(Path confDir) throws IOException {
        List<Path> files = new ArrayList<>();
        List<SkippedEntry> skipped = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(confDir)) {
            for (Path path : stream) {
                String name = path.getFileName().toString();
                if (Files.isDirectory(path)) {
                    skipped.add(new SkippedEntry(name, "directory"));
                    continue;
                }
                if (!Files.isRegularFile(path)) {
                    skipped.add(new SkippedEntry(name, "not a regular file"));
                    continue;
                }
                if (BINARY_OR_ARCHIVE.matcher(name).matches()) {
                    skipped.add(new SkippedEntry(name, "archive/binary extension"));
                    continue;
                }
                if (TEMPLATE.matcher(name).matches()) {
                    skipped.add(new SkippedEntry(name, "*.template"));
                    continue;
                }
                files.add(path);
            }
        }
        return new ListedConfFiles(files, skipped);
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

    static final class ListedConfFiles {
        private final List<Path> files;
        private final List<SkippedEntry> skipped;

        private ListedConfFiles(List<Path> files, List<SkippedEntry> skipped) {
            this.files = files;
            this.skipped = skipped;
        }
    }

    static final class SkippedEntry {
        private final String name;
        private final String reason;

        private SkippedEntry(String name, String reason) {
            this.name = name;
            this.reason = reason;
        }
    }
}
