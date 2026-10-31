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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConfDirFilesTest {

    @TempDir Path temporary;

    @Test
    void filtersArchivesTemplatesAndKeepsYamlProperties() throws Exception {
        Files.write(
                temporary.resolve("seatunnel.yaml"),
                "seatunnel: {}\n".getBytes(StandardCharsets.UTF_8));
        Files.write(
                temporary.resolve("hive-site.xml"),
                "<configuration/>\n".getBytes(StandardCharsets.UTF_8));
        Files.write(
                temporary.resolve("metrics.properties"), "a=b\n".getBytes(StandardCharsets.UTF_8));
        Files.write(temporary.resolve("skip.jar"), new byte[] {1, 2, 3});
        Files.write(temporary.resolve("skip.gz"), new byte[] {1});
        Files.write(temporary.resolve("ignored.template"), "x".getBytes(StandardCharsets.UTF_8));
        Files.createDirectory(temporary.resolve("subdir"));
        Files.write(
                temporary.resolve("subdir/nested.yaml"), "nested".getBytes(StandardCharsets.UTF_8));

        Map<String, String> loaded = ConfDirFiles.load(temporary, 1048576L);
        assertTrue(loaded.containsKey("seatunnel.yaml"));
        assertTrue(loaded.containsKey("hive-site.xml"));
        assertTrue(loaded.containsKey("metrics.properties"));
        assertFalse(loaded.containsKey("skip.jar"));
        assertFalse(loaded.containsKey("skip.gz"));
        assertFalse(loaded.containsKey("ignored.template"));
        assertFalse(loaded.containsKey("nested.yaml"));
    }

    @Test
    void failsFastWhenPayloadExceedsMaxSizeAndSkipsNonUtf8() throws Exception {
        Files.write(temporary.resolve("a.txt"), "aa".getBytes(StandardCharsets.UTF_8));
        Files.write(temporary.resolve("bb.txt"), "bbbb".getBytes(StandardCharsets.UTF_8));
        Files.write(temporary.resolve("bad.bin"), new byte[] {(byte) 0xff, (byte) 0xfe, 0x01});

        // maxSize 10: a.txt fits (name 5 + content 2 = 7), bb.txt would exceed (used 7 + 6+4).
        IllegalArgumentException failure =
                assertThrows(
                        IllegalArgumentException.class, () -> ConfDirFiles.load(temporary, 10L));
        assertTrue(failure.getMessage().contains("kubernetes.configmap.max-size"));
        assertTrue(failure.getMessage().contains("bb.txt"));
    }

    @Test
    void accountsUtf8BytesForMultibyteContent() throws Exception {
        // Two UTF-8 bytes per character: "€" is 3 bytes; use "ä" (2 bytes) for a clear delta.
        Files.write(temporary.resolve("x.txt"), "ää".getBytes(StandardCharsets.UTF_8));
        // name "x.txt" = 5 bytes, content = 4 bytes, total 9 — fits in 9, fails in 8.
        Map<String, String> loaded = ConfDirFiles.load(temporary, 9L);
        assertEquals(Map.of("x.txt", "ää"), loaded);
        assertThrows(IllegalArgumentException.class, () -> ConfDirFiles.load(temporary, 8L));
    }

    @Test
    void configMapNameRespectsDnsLabelLength() {
        String shortName = ConfDirFiles.configMapName("demo-batch-abc123def456");
        assertEquals("demo-batch-abc123def456-conf-map", shortName);
        assertTrue(shortName.length() <= KubernetesConstants.DNS_LABEL_MAX_LENGTH);

        String longId = "a".repeat(80);
        String truncated = ConfDirFiles.configMapName(longId);
        assertTrue(truncated.length() <= KubernetesConstants.DNS_LABEL_MAX_LENGTH);
        assertTrue(truncated.endsWith("-conf-map"));
    }
}
