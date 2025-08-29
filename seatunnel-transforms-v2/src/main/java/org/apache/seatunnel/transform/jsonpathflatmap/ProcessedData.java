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
package org.apache.seatunnel.transform.jsonpathflatmap;

import lombok.Data;
import lombok.Getter;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/** Encapsulates processed data */
@Getter
@Data
public class ProcessedData {
    private final Map<String, List<Object>> arrayDataMap;
    private final Map<String, Object> singleFieldDataMap;

    public ProcessedData(
            Map<String, List<Object>> arrayDataMap, Map<String, Object> singleFieldDataMap) {
        this.arrayDataMap = arrayDataMap != null ? arrayDataMap : Collections.emptyMap();
        this.singleFieldDataMap =
                singleFieldDataMap != null ? singleFieldDataMap : Collections.emptyMap();
    }

    public static ProcessedData empty() {
        return new ProcessedData(Collections.emptyMap(), Collections.emptyMap());
    }

    public boolean isEmpty() {
        return arrayDataMap.isEmpty() && singleFieldDataMap.isEmpty();
    }
}
