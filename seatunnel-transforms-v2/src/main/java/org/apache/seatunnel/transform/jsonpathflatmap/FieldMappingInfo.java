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

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.format.json.JsonToRowConverters;
import org.apache.seatunnel.transform.common.ErrorHandleWay;
import org.apache.seatunnel.transform.exception.ErrorDataTransformException;
import org.apache.seatunnel.transform.jsonpathflatmap.JsonPathFlatMapConfig.FieldMergeStrategy;
import org.apache.seatunnel.transform.jsonpathflatmap.JsonPathFlatMapConfig.FillStrategy;
import org.apache.seatunnel.transform.jsonpathflatmap.JsonPathFlatMapConfig.OutputFieldConfig;

import com.jayway.jsonpath.JsonPath;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.apache.seatunnel.transform.exception.JsonPathFlatMapErrorCode.OUTPUT_FIELD_EXTRACTION_ERROR;

/** Field mapping information management class */
@Slf4j
public class FieldMappingInfo {

    private static final Map<String, JsonPath> JSON_PATH_CACHE = new ConcurrentHashMap<>();

    private final SeaTunnelRowType inputRowType;
    private final List<OutputFieldConfig> outputFields;
    private final FieldMergeStrategy fieldMergeStrategy;
    private final List<String> preserveSourceFields;

    private final List<Column> outputColumns;
    private final List<Boolean> isSourceField;
    private final List<Integer> sourceFieldIndices;

    public FieldMappingInfo(
            SeaTunnelRowType inputRowType,
            List<OutputFieldConfig> outputFields,
            FieldMergeStrategy fieldMergeStrategy,
            List<String> preserveSourceFields) {

        this.inputRowType = inputRowType;
        this.outputFields = outputFields;
        this.fieldMergeStrategy = fieldMergeStrategy;
        this.preserveSourceFields = preserveSourceFields;
        this.outputColumns = new ArrayList<>();
        this.isSourceField = new ArrayList<>();
        this.sourceFieldIndices = new ArrayList<>();

        initFieldMappings();
    }

    private void initFieldMappings() {
        switch (fieldMergeStrategy) {
            case APPEND_NEW:
                initAppendNewMappings();
                break;
            case REPLACE_ALL:
                initReplaceAllMappings();
                break;
            case KEEP_SOURCE_ORDER:
                initKeepSourceOrderMappings();
                break;
        }
    }

    private void initAppendNewMappings() {
        // Add all source fields
        for (int i = 0; i < inputRowType.getFieldNames().length; i++) {
            String fieldName = inputRowType.getFieldNames()[i];
            SeaTunnelDataType<?> fieldType = inputRowType.getFieldType(i);
            outputColumns.add(
                    PhysicalColumn.of(fieldName, fieldType, (Long) null, true, null, null));
            isSourceField.add(true);
            sourceFieldIndices.add(i);
        }

        // Add new fields
        for (OutputFieldConfig outputField : outputFields) {
            outputColumns.add(outputField.getDestColumn());
            isSourceField.add(false);
            sourceFieldIndices.add(-1);
        }
    }

    private void initReplaceAllMappings() {
        // Add only new fields
        for (OutputFieldConfig outputField : outputFields) {
            outputColumns.add(outputField.getDestColumn());
            isSourceField.add(false);
            sourceFieldIndices.add(-1);
        }
    }

    private void initKeepSourceOrderMappings() {
        Set<String> outputFieldNames = new HashSet<>();
        for (OutputFieldConfig outputField : outputFields) {
            outputFieldNames.add(outputField.getDestField());
        }

        Set<String> preserveFieldNames = new HashSet<>();
        if (preserveSourceFields != null) {
            preserveFieldNames.addAll(preserveSourceFields);
        }

        // Process in source field order
        for (int i = 0; i < inputRowType.getFieldNames().length; i++) {
            String fieldName = inputRowType.getFieldNames()[i];

            if (outputFieldNames.contains(fieldName)) {
                // Replace with output field
                OutputFieldConfig outputField = findOutputFieldByName(fieldName);
                if (outputField != null) {
                    outputColumns.add(outputField.getDestColumn());
                    isSourceField.add(false);
                    sourceFieldIndices.add(-1);
                }
            } else if (preserveSourceFields == null || preserveFieldNames.contains(fieldName)) {
                // Keep source field
                SeaTunnelDataType<?> fieldType = inputRowType.getFieldType(i);
                outputColumns.add(
                        PhysicalColumn.of(fieldName, fieldType, (Long) null, true, null, null));
                isSourceField.add(true);
                sourceFieldIndices.add(i);
            }
        }

        // Add new fields not in source fields
        for (OutputFieldConfig outputField : outputFields) {
            boolean existsInSource = false;
            for (String sourceFieldName : inputRowType.getFieldNames()) {
                if (sourceFieldName.equals(outputField.getDestField())) {
                    existsInSource = true;
                    break;
                }
            }

            if (!existsInSource) {
                outputColumns.add(outputField.getDestColumn());
                isSourceField.add(false);
                sourceFieldIndices.add(-1);
            }
        }
    }

    /** Find output field configuration by field name */
    private OutputFieldConfig findOutputFieldByName(String fieldName) {
        for (OutputFieldConfig outputField : outputFields) {
            if (outputField.getDestField().equals(fieldName)) {
                return outputField;
            }
        }
        return null;
    }

    /** Create output table schema */
    public TableSchema createOutputTableSchema(TableSchema inputTableSchema) {
        List<ConstraintKey> copyConstraintKeys =
                inputTableSchema.getConstraintKeys().stream()
                        .map(ConstraintKey::copy)
                        .collect(Collectors.toList());

        return TableSchema.builder()
                .constraintKey(copyConstraintKeys)
                .columns(outputColumns)
                .primaryKey(
                        inputTableSchema.getPrimaryKey() == null
                                ? null
                                : inputTableSchema.getPrimaryKey().copy())
                .build();
    }

    /** Create output field array */
    public Object[] createOutputFields(
            SeaTunnelRow inputRow,
            ProcessedData processedData,
            Map<String, Integer> arrayIndexMap,
            JsonToRowConverters.JsonToObjectConverter[] converters,
            ErrorHandleWay globalErrorHandleWay) {

        Object[] outputFields = new Object[outputColumns.size()];
        int converterIndex = 0;

        for (int i = 0; i < outputColumns.size(); i++) {
            if (isSourceField.get(i)) {
                // Source field: get directly from input row
                int sourceIndex = sourceFieldIndices.get(i);
                outputFields[i] = inputRow.getField(sourceIndex);
            } else {
                // Output field: extract from array data
                OutputFieldConfig outputFieldConfig = this.outputFields.get(converterIndex);

                try {
                    Object fieldValue =
                            extractOutputFieldValue(
                                    outputFieldConfig, processedData, arrayIndexMap);

                    // Use converter to transform data type, pass correct field name
                    JsonNode jsonNode = JsonUtils.toJsonNode(fieldValue);
                    outputFields[i] =
                            converters[converterIndex].convert(
                                    jsonNode, outputFieldConfig.getDestField());
                    converterIndex++;
                } catch (Exception e) {
                    if (!handleFieldError(outputFieldConfig, globalErrorHandleWay, e)) {
                        return null; // Skip entire row
                    }
                    outputFields[i] = null;
                    converterIndex++;
                }
            }
        }

        return outputFields;
    }

    /** Field error handling method */
    private boolean handleFieldError(
            OutputFieldConfig outputFieldConfig, ErrorHandleWay globalErrorHandleWay, Exception e) {
        ErrorHandleWay errorHandleWay =
                outputFieldConfig.getErrorHandleWay() != null
                        ? outputFieldConfig.getErrorHandleWay()
                        : globalErrorHandleWay;

        if (errorHandleWay != null && errorHandleWay.allowSkip()) {
            return true; // Continue processing other fields
        } else if (errorHandleWay != null && errorHandleWay.allowSkipThisRow()) {
            return false; // Skip entire row
        } else {
            throw new ErrorDataTransformException(
                    errorHandleWay,
                    OUTPUT_FIELD_EXTRACTION_ERROR,
                    String.format(
                            "JsonPath FlatMap transform error, field: %s, error: %s",
                            outputFieldConfig.getDestField(), e.getMessage()));
        }
    }

    /** Extract output field value - get from preprocessed data */
    private Object extractOutputFieldValue(
            OutputFieldConfig outputFieldConfig,
            ProcessedData processedData,
            Map<String, Integer> arrayIndexMap) {

        String fromArray = outputFieldConfig.getFromArray();
        String destField = outputFieldConfig.getDestField();

        // src_field and from_array are mutually exclusive
        if (fromArray == null || fromArray.trim().isEmpty()) {
            return processedData.getSingleFieldDataMap().get(destField);
        }

        // Get from array data
        Map<String, List<Object>> arrayDataMap = processedData.getArrayDataMap();
        List<Object> arrayData = arrayDataMap.get(fromArray);
        if (arrayData == null || arrayData.isEmpty()) {
            return null;
        }

        Integer arrayIndex = arrayIndexMap.get(fromArray);
        if (arrayIndex == null || arrayIndex == -1) {
            return null;
        }

        if (arrayIndex >= arrayData.size()) {
            FillStrategy fillStrategy = outputFieldConfig.getFillStrategy();
            if (fillStrategy != null) {
                switch (fillStrategy) {
                    case REPEAT_LAST:
                        return arrayData.get(arrayData.size() - 1);
                    case REPEAT_FIRST:
                        return arrayData.get(0);
                    case NULL:
                        return null;
                    default:
                        throw new IllegalArgumentException(
                                "Unsupported field-level fill strategy: "
                                        + fillStrategy
                                        + " for field: "
                                        + outputFieldConfig.getDestField()
                                        + ". Please use one of: NULL, REPEAT_LAST, REPEAT_FIRST");
                }
            } else {
                throw new IllegalArgumentException(
                        "Array index out of bounds for field: "
                                + outputFieldConfig.getDestField()
                                + ". Array size: "
                                + arrayData.size()
                                + ", requested index: "
                                + arrayIndex
                                + ". Please configure fill_strategy with one of: NULL, REPEAT_LAST, REPEAT_FIRST");
            }
        }

        // Normally, the index should be within valid range as out-of-bounds cases are handled above
        Object arrayItem = arrayData.get(arrayIndex);
        if (arrayItem == null) {
            return null;
        }

        String itemPath = outputFieldConfig.getItemPath();
        if ("$".equals(itemPath) || itemPath.isEmpty()) {
            return arrayItem;
        } else {
            JsonPath jsonPath = JSON_PATH_CACHE.computeIfAbsent(itemPath, JsonPath::compile);
            return jsonPath.read(arrayItem);
        }
    }
}
