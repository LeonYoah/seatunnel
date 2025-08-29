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

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.format.json.JsonToRowConverters;
import org.apache.seatunnel.transform.jsonpathflatmap.JsonPathFlatMapConfig.OutputFieldConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Row builder - responsible for building output rows based on array data and flatten strategy */
public class RowBuilder {

    private final JsonPathFlatMapConfig config;
    private final FieldMappingInfo fieldMappingInfo;
    private final JsonToRowConverters.JsonToObjectConverter[] converters;

    public RowBuilder(JsonPathFlatMapConfig config, SeaTunnelRowType inputRowType) {
        this.config = config;
        this.fieldMappingInfo =
                new FieldMappingInfo(
                        inputRowType,
                        config.getOutputFields(),
                        config.getFieldMergeStrategy(),
                        config.getPreserveSourceFields());

        // Initialize converters
        JsonToRowConverters jsonToRowConverters = new JsonToRowConverters(false, false);
        this.converters =
                config.getOutputFields().stream()
                        .map(OutputFieldConfig::getDestColumn)
                        .map(Column::getDataType)
                        .map(jsonToRowConverters::createConverter)
                        .toArray(JsonToRowConverters.JsonToObjectConverter[]::new);
    }

    /** Build output rows based on processed data */
    public List<SeaTunnelRow> buildRows(SeaTunnelRow inputRow, ProcessedData processedData) {
        Map<String, List<Object>> arrayDataMap = processedData.getArrayDataMap();

        if (arrayDataMap.isEmpty()) {
            // When there's no array data, generate one output row containing source fields and
            // single field data
            SeaTunnelRow outputRow =
                    createOutputRow(inputRow, processedData, Collections.emptyMap());
            return outputRow != null
                    ? Collections.singletonList(outputRow)
                    : Collections.emptyList();
        }

        switch (config.getFlattenStrategy()) {
            case CARTESIAN_PRODUCT:
                return buildCartesianProductRows(inputRow, processedData);
            case MAX_LENGTH_ALIGN:
                return buildMaxLengthAlignRows(inputRow, processedData);
            case PARALLEL_EXPAND:
                return buildParallelExpandRows(inputRow, processedData);
            default:
                return Collections.singletonList(buildSingleRow(inputRow, processedData));
        }
    }

    private List<SeaTunnelRow> buildCartesianProductRows(
            SeaTunnelRow inputRow, ProcessedData processedData) {
        Map<String, List<Object>> arrayDataMap = processedData.getArrayDataMap();
        List<SeaTunnelRow> resultRows = new ArrayList<>();
        List<List<Integer>> combinations = generateCartesianCombinations(arrayDataMap);

        if (combinations.isEmpty()) {
            // When there's no array data (possibly due to array field errors being skipped), still
            // generate one output row with source fields, output fields as null
            Map<String, Integer> emptyIndexMap = new HashMap<>();
            SeaTunnelRow outputRow = createOutputRow(inputRow, processedData, emptyIndexMap);
            if (outputRow != null) {
                resultRows.add(outputRow);
            }
        } else {
            for (List<Integer> combination : combinations) {
                Map<String, Integer> arrayIndexMap =
                        createIndexMapFromCombination(arrayDataMap, combination);
                SeaTunnelRow outputRow = createOutputRow(inputRow, processedData, arrayIndexMap);
                if (outputRow != null) {
                    resultRows.add(outputRow);
                }
            }
        }

        return resultRows;
    }

    private List<SeaTunnelRow> buildMaxLengthAlignRows(
            SeaTunnelRow inputRow, ProcessedData processedData) {
        Map<String, List<Object>> arrayDataMap = processedData.getArrayDataMap();
        int maxLength = arrayDataMap.values().stream().mapToInt(List::size).max().orElse(0);

        if (maxLength == 0) {
            // When there's no array data (possibly due to array field errors being skipped), still
            // generate one output row with source fields, output fields as null
            Map<String, Integer> emptyIndexMap = new HashMap<>();
            SeaTunnelRow outputRow = createOutputRow(inputRow, processedData, emptyIndexMap);
            return outputRow != null
                    ? Collections.singletonList(outputRow)
                    : Collections.emptyList();
        }

        List<SeaTunnelRow> resultRows = new ArrayList<>();
        for (int i = 0; i < maxLength; i++) {
            Map<String, Integer> arrayIndexMap = createIndexMapAtIndex(arrayDataMap, i);
            SeaTunnelRow outputRow = createOutputRow(inputRow, processedData, arrayIndexMap);
            if (outputRow != null) {
                resultRows.add(outputRow);
            }
        }

        return resultRows;
    }

    private List<SeaTunnelRow> buildParallelExpandRows(
            SeaTunnelRow inputRow, ProcessedData processedData) {
        Map<String, List<Object>> arrayDataMap = processedData.getArrayDataMap();
        int minLength = arrayDataMap.values().stream().mapToInt(List::size).min().orElse(0);

        if (minLength == 0) {
            // When there's no array data (possibly due to array field errors being skipped), still
            // generate one output row with source fields, other output fields as null
            Map<String, Integer> emptyIndexMap = new HashMap<>();
            SeaTunnelRow outputRow = createOutputRow(inputRow, processedData, emptyIndexMap);
            return outputRow != null
                    ? Collections.singletonList(outputRow)
                    : Collections.emptyList();
        }

        List<SeaTunnelRow> resultRows = new ArrayList<>();
        for (int i = 0; i < minLength; i++) {
            Map<String, Integer> arrayIndexMap = new HashMap<>();
            for (String key : arrayDataMap.keySet()) {
                arrayIndexMap.put(key, i);
            }

            SeaTunnelRow outputRow = createOutputRow(inputRow, processedData, arrayIndexMap);
            if (outputRow != null) {
                resultRows.add(outputRow);
            }
        }

        return resultRows;
    }

    private SeaTunnelRow buildSingleRow(SeaTunnelRow inputRow, ProcessedData processedData) {
        Map<String, Integer> emptyIndexMap = new HashMap<>();
        return createOutputRow(inputRow, processedData, emptyIndexMap);
    }

    private SeaTunnelRow createOutputRow(
            SeaTunnelRow inputRow,
            ProcessedData processedData,
            Map<String, Integer> arrayIndexMap) {
        Object[] outputFields =
                fieldMappingInfo.createOutputFields(
                        inputRow,
                        processedData,
                        arrayIndexMap,
                        converters,
                        config.getErrorHandleWay());

        if (outputFields == null) {
            return null;
        }

        SeaTunnelRow outputRow = new SeaTunnelRow(outputFields);
        outputRow.setTableId(inputRow.getTableId());
        outputRow.setRowKind(inputRow.getRowKind());

        return outputRow;
    }

    private List<List<Integer>> generateCartesianCombinations(
            Map<String, List<Object>> arrayDataMap) {
        List<List<Integer>> combinations = new ArrayList<>();
        List<String> keys = new ArrayList<>(arrayDataMap.keySet());
        Collections.sort(keys);

        if (keys.isEmpty()) {
            return combinations;
        }

        generateCombinationsRecursive(arrayDataMap, keys, 0, new ArrayList<>(), combinations);
        return combinations;
    }

    private void generateCombinationsRecursive(
            Map<String, List<Object>> arrayDataMap,
            List<String> keys,
            int keyIndex,
            List<Integer> currentCombination,
            List<List<Integer>> combinations) {
        if (keyIndex >= keys.size()) {
            combinations.add(new ArrayList<>(currentCombination));
            return;
        }

        String key = keys.get(keyIndex);
        List<Object> array = arrayDataMap.get(key);

        for (int i = 0; i < array.size(); i++) {
            currentCombination.add(i);
            generateCombinationsRecursive(
                    arrayDataMap, keys, keyIndex + 1, currentCombination, combinations);
            currentCombination.remove(currentCombination.size() - 1);
        }
    }

    private Map<String, Integer> createIndexMapFromCombination(
            Map<String, List<Object>> arrayDataMap, List<Integer> combination) {
        Map<String, Integer> arrayIndexMap = new HashMap<>();
        List<String> keys = new ArrayList<>(arrayDataMap.keySet());
        Collections.sort(keys);

        for (int i = 0; i < keys.size() && i < combination.size(); i++) {
            arrayIndexMap.put(keys.get(i), combination.get(i));
        }

        return arrayIndexMap;
    }

    private Map<String, Integer> createIndexMapAtIndex(
            Map<String, List<Object>> arrayDataMap, int index) {
        Map<String, Integer> arrayIndexMap = new HashMap<>();

        for (Map.Entry<String, List<Object>> entry : arrayDataMap.entrySet()) {
            String key = entry.getKey();
            List<Object> array = entry.getValue();

            int actualIndex = getActualIndex(array, index);
            arrayIndexMap.put(key, actualIndex);
        }

        return arrayIndexMap;
    }

    private int getActualIndex(List<Object> array, int requestedIndex) {
        if (array.isEmpty()) {
            return -1;
        }

        if (requestedIndex < array.size()) {
            return requestedIndex;
        }

        // When array lengths are inconsistent, handle according to fill strategy
        switch (config.getFillStrategy()) {
            case NULL:
                return -1;
            case REPEAT_LAST:
                return array.size() - 1;
            case REPEAT_FIRST:
                return 0;
            default:
                throw new IllegalArgumentException(
                        "Unsupported fill strategy: "
                                + config.getFillStrategy()
                                + ". Please use one of: NULL, REPEAT_LAST, REPEAT_FIRST");
        }
    }

    public TableSchema createOutputTableSchema(TableSchema inputTableSchema) {
        return fieldMappingInfo.createOutputTableSchema(inputTableSchema);
    }
}
