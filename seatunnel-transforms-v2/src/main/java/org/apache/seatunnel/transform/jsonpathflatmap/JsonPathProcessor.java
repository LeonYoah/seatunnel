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

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.transform.common.ErrorHandleWay;
import org.apache.seatunnel.transform.exception.ErrorDataTransformException;
import org.apache.seatunnel.transform.exception.TransformCommonError;
import org.apache.seatunnel.transform.jsonpathflatmap.JsonPathFlatMapConfig.ArrayFlattenFieldConfig;

import org.apache.commons.lang3.StringEscapeUtils;

import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.JsonPathException;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.seatunnel.transform.exception.JsonPathFlatMapErrorCode.ARRAY_EXTRACTION_ERROR;
import static org.apache.seatunnel.transform.exception.JsonPathFlatMapErrorCode.JSON_UNESCAPE_ERROR;

/** JsonPath data processor - responsible for extracting array data from input rows */
@Slf4j
public class JsonPathProcessor {

    private static final Map<String, JsonPath> JSON_PATH_CACHE = new ConcurrentHashMap<>();

    private final JsonPathFlatMapConfig config;
    private final SeaTunnelRowType inputRowType;

    public JsonPathProcessor(JsonPathFlatMapConfig config, SeaTunnelRowType inputRowType) {
        this.config = config;
        this.inputRowType = inputRowType;
    }

    /** Extract all data from input row (array data + single field data) */
    public ProcessedData extractAllData(SeaTunnelRow inputRow) {
        Map<String, List<Object>> arrayDataMap = new HashMap<>();
        Map<String, Object> singleFieldDataMap = new HashMap<>();

        // Extract array data
        for (ArrayFlattenFieldConfig fieldConfig : config.getArrayFlattenFields()) {
            try {
                List<Object> arrayData = extractFieldArrayData(inputRow, fieldConfig);
                arrayDataMap.put(fieldConfig.getFieldKey(), arrayData);
            } catch (JsonPathException e) {
                if (!handleFieldError(fieldConfig.getErrorHandleWay(), fieldConfig.toString(), e)) {
                    return ProcessedData.empty(); // Skip entire row
                }
                arrayDataMap.put(fieldConfig.getFieldKey(), Collections.emptyList());
            }
        }

        // Extract single field data (fields specified by src_field)
        for (JsonPathFlatMapConfig.OutputFieldConfig outputField : config.getOutputFields()) {
            String srcField = outputField.getSrcField();
            String fromArray = outputField.getFromArray();

            // src_field and from_array are mutually exclusive
            if (srcField != null
                    && !srcField.trim().isEmpty()
                    && (fromArray == null || fromArray.trim().isEmpty())) {

                String fieldKey = outputField.getDestField();
                try {
                    Object fieldValue =
                            extractSingleFieldValue(
                                    inputRow,
                                    srcField,
                                    outputField.getItemPath(),
                                    outputField.getErrorHandleWay());
                    singleFieldDataMap.put(fieldKey, fieldValue);
                } catch (Exception e) {
                    String fieldDescription =
                            String.format(
                                    "sourceField: %s, itemPath: %s",
                                    outputField.getSrcField(), outputField.getItemPath());
                    if (!handleFieldError(outputField.getErrorHandleWay(), fieldDescription, e)) {
                        return ProcessedData.empty(); // Skip entire row
                    }
                    singleFieldDataMap.put(fieldKey, null); // Skip this field, set to null
                }
            }
        }

        return new ProcessedData(arrayDataMap, singleFieldDataMap);
    }

    /**
     * Extract single field value from specified source field (supports non-array field extraction)
     */
    public Object extractSingleFieldValue(
            SeaTunnelRow inputRow,
            String sourceFieldName,
            String jsonPath,
            ErrorHandleWay errorHandleWay) {
        // Get source field value
        int srcFieldIndex = inputRowType.indexOf(sourceFieldName);
        if (srcFieldIndex == -1) {
            throw TransformCommonError.cannotFindInputFieldError(
                    "JsonPathFlatMap", sourceFieldName);
        }

        Object srcValue = inputRow.getField(srcFieldIndex);
        if (srcValue == null) {
            return null;
        }

        String jsonString = convertToJsonString(srcValue, inputRowType.getFieldType(srcFieldIndex));

        if ("$".equals(jsonPath) || jsonPath.isEmpty()) {
            return JsonUtils.parseObject(jsonString);
        } else {
            JsonPath compiledPath = JSON_PATH_CACHE.computeIfAbsent(jsonPath, JsonPath::compile);
            return compiledPath.read(jsonString);
        }
    }

    private List<Object> extractFieldArrayData(
            SeaTunnelRow inputRow, ArrayFlattenFieldConfig fieldConfig) {
        int srcFieldIndex = inputRowType.indexOf(fieldConfig.getSrcField());
        if (srcFieldIndex == -1) {
            throw TransformCommonError.cannotFindInputFieldError(
                    "JsonPathFlatMap", fieldConfig.getSrcField());
        }

        Object srcValue = inputRow.getField(srcFieldIndex);
        if (srcValue == null) {
            return Collections.emptyList();
        }

        // Convert to JSON string and extract array
        String jsonString = convertToJsonString(srcValue, inputRowType.getFieldType(srcFieldIndex));

        if (fieldConfig.isRootArray()) {
            return extractRootArray(jsonString);
        } else {
            JsonPath jsonPath =
                    JSON_PATH_CACHE.computeIfAbsent(fieldConfig.getArrayPath(), JsonPath::compile);
            Object result = jsonPath.read(jsonString);
            return processArrayResult(result);
        }
    }

    private String convertToJsonString(Object value, SeaTunnelDataType<?> dataType) {
        String jsonString;
        switch (dataType.getSqlType()) {
            case STRING:
                jsonString = value.toString();
                break;
            case BYTES:
                jsonString = new String((byte[]) value);
                break;
            case ARRAY:
            case MAP:
                jsonString = JsonUtils.toJsonString(value);
                break;
            case ROW:
                SeaTunnelRow row = (SeaTunnelRow) value;
                jsonString = JsonUtils.toJsonString(row.getFields());
                break;
            default:
                throw CommonError.unsupportedDataType(
                        "JsonPathFlatMap", dataType.getSqlType().toString(), "source field");
        }

        if (config.isUnescapeJson()
                && (dataType.getSqlType() == SqlType.STRING
                        || dataType.getSqlType() == SqlType.BYTES)) {
            jsonString = unescapeJsonString(jsonString);
        }

        return jsonString;
    }

    private String unescapeJsonString(String jsonString) {
        if (jsonString == null || jsonString.isEmpty()) {
            return jsonString;
        }

        try {
            return StringEscapeUtils.unescapeJson(jsonString);
        } catch (Exception e) {
            if (config.getErrorHandleWay().allowSkip()) {
                log.warn(
                        "Failed to unescape JSON string: {}, using original string", jsonString, e);
                return jsonString;
            } else {
                throw new ErrorDataTransformException(
                        config.getErrorHandleWay(),
                        JSON_UNESCAPE_ERROR,
                        String.format(
                                "JSON unescape failed for string: %s, error: %s",
                                jsonString, e.getMessage()));
            }
        }
    }

    private List<Object> extractRootArray(String jsonString) {
        JsonPath rootArrayPath = JSON_PATH_CACHE.computeIfAbsent("$[*]", JsonPath::compile);
        Object result = rootArrayPath.read(jsonString);

        if (result instanceof List) {
            return (List<Object>) result;
        } else {
            JsonPath rootPath = JSON_PATH_CACHE.computeIfAbsent("$", JsonPath::compile);
            Object rootResult = rootPath.read(jsonString);
            if (rootResult instanceof List) {
                return (List<Object>) rootResult;
            } else {
                return Collections.singletonList(rootResult);
            }
        }
    }

    private List<Object> processArrayResult(Object result) {
        if (result instanceof List) {
            return (List<Object>) result;
        } else if (result != null) {
            return Collections.singletonList(result);
        } else {
            return Collections.emptyList();
        }
    }

    private boolean handleFieldError(
            ErrorHandleWay errorHandleWay, String fieldDescription, Exception e) {
        ErrorHandleWay effectiveErrorHandleWay =
                errorHandleWay != null ? errorHandleWay : config.getErrorHandleWay();

        if (effectiveErrorHandleWay.allowSkip()) {
            log.warn("JsonPath extract error, skip this field, field: {}", fieldDescription, e);
            return true; // Skip this field, continue processing other fields
        } else if (effectiveErrorHandleWay.allowSkipThisRow()) {
            log.warn("JsonPath extract error, skip this row, field: {}", fieldDescription, e);
            return false; // Skip entire row
        } else {
            throw new ErrorDataTransformException(
                    effectiveErrorHandleWay,
                    ARRAY_EXTRACTION_ERROR,
                    String.format(
                            "JsonPath extract error, field: %s, error: %s",
                            fieldDescription, e.getMessage()));
        }
    }
}
