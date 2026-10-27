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
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConfDirFilesTest {

    @TempDir Path temporary;

    @Test
    void filtersArchivesTemplatesAndKeepsYamlProperties() throws Exception {
        Files.writeString(temporary.resolve("seatunnel.yaml"), "seatunnel: {}\n");
        Files.writeString(temporary.resolve("hive-site.xml"), "<configuration/>\n");
        Files.writeString(temporary.resolve("metrics.properties"), "a=b\n");
        Files.write(temporary.resolve("skip.jar"), new byte[] {1, 2, 3});
        Files.write(temporary.resolve("skip.gz"), new byte[] {1});
        Files.writeString(temporary.resolve("ignored.template"), "x");
        Files.createDirectory(temporary.resolve("subdir"));
        Files.writeString(temporary.resolve("subdir/nested.yaml"), "nested");

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
    void truncatesByAscendingNamePlusSizeAndSkipsNonUtf8() throws Exception {
        Files.writeString(temporary.resolve("a.txt"), "aa");
        Files.writeString(temporary.resolve("bb.txt"), "bbbb");
        Files.write(temporary.resolve("bad.bin"), new byte[] {(byte) 0xff, (byte) 0xfe, 0x01});

        // maxSize 10: include a.txt (name 5 + content 2 = 7), skip bb.txt (6+4=10 not < 10)
        Map<String, String> loaded = ConfDirFiles.load(temporary, 10L);
        assertEquals(Map.of("a.txt", "aa"), loaded);
        assertFalse(loaded.containsKey("bad.bin"));
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
