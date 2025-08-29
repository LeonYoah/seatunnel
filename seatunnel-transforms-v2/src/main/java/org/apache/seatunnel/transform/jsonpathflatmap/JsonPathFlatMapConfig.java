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

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.SeaTunnelDataTypeConvertorUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.transform.common.ErrorHandleWay;
import org.apache.seatunnel.transform.common.TransformCommonOptions;
import org.apache.seatunnel.transform.exception.TransformCommonError;
import org.apache.seatunnel.transform.exception.TransformException;

import org.apache.commons.lang3.StringUtils;

import lombok.Getter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.seatunnel.transform.exception.JsonPathTransformErrorCode.COLUMNS_MUST_NOT_EMPTY;

@Getter
public class JsonPathFlatMapConfig implements Serializable {

    // Array flatten field configurations
    public static final Option<List<Map<String, Object>>> ARRAY_FLATTEN_FIELDS =
            Options.key("array_flatten_fields")
                    .type(new TypeReference<List<Map<String, Object>>>() {})
                    .noDefaultValue()
                    .withDescription("Array flatten field configurations");

    // Output field configurations
    public static final Option<List<Map<String, Object>>> OUTPUT_FIELDS =
            Options.key("output_fields")
                    .type(new TypeReference<List<Map<String, Object>>>() {})
                    .noDefaultValue()
                    .withDescription("Output field configurations");

    // Field merge strategy
    public static final Option<String> FIELD_MERGE_STRATEGY =
            Options.key("field_merge_strategy")
                    .stringType()
                    .defaultValue("APPEND_NEW")
                    .withDescription(
                            "Field merge strategy: APPEND_NEW, REPLACE_ALL, KEEP_SOURCE_ORDER");

    // Source fields to preserve list
    public static final Option<List<String>> PRESERVE_SOURCE_FIELDS =
            Options.key("preserve_source_fields")
                    .listType()
                    .noDefaultValue()
                    .withDescription(
                            "Source fields to preserve when using KEEP_SOURCE_ORDER strategy");

    // Array flatten strategy
    public static final Option<String> FLATTEN_STRATEGY =
            Options.key("flatten_strategy")
                    .stringType()
                    .defaultValue("MAX_LENGTH_ALIGN")
                    .withDescription(
                            "Array flatten strategy: CARTESIAN_PRODUCT, MAX_LENGTH_ALIGN, PARALLEL_EXPAND");

    // Fill strategy
    public static final Option<String> FILL_STRATEGY =
            Options.key("fill_strategy")
                    .stringType()
                    .defaultValue("NULL")
                    .withDescription(
                            "Fill strategy for array length mismatch: NULL, REPEAT_LAST, REPEAT_FIRST");

    // JSON unescape switch
    public static final Option<Boolean> UNESCAPE_JSON =
            Options.key("unescape_json")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to unescape JSON strings before processing. Default is false.");

    private final List<ArrayFlattenFieldConfig> arrayFlattenFields;
    private final List<OutputFieldConfig> outputFields;
    private final FieldMergeStrategy fieldMergeStrategy;
    private final List<String> preserveSourceFields;
    private final ArrayFlattenStrategy flattenStrategy;
    private final FillStrategy fillStrategy;
    private final boolean unescapeJson;
    private final ErrorHandleWay errorHandleWay;

    public JsonPathFlatMapConfig(
            List<ArrayFlattenFieldConfig> arrayFlattenFields,
            List<OutputFieldConfig> outputFields,
            FieldMergeStrategy fieldMergeStrategy,
            List<String> preserveSourceFields,
            ArrayFlattenStrategy flattenStrategy,
            FillStrategy fillStrategy,
            boolean unescapeJson,
            ErrorHandleWay errorHandleWay) {
        this.arrayFlattenFields = arrayFlattenFields;
        this.outputFields = outputFields;
        this.fieldMergeStrategy = fieldMergeStrategy;
        this.preserveSourceFields = preserveSourceFields;
        this.flattenStrategy = flattenStrategy;
        this.fillStrategy = fillStrategy;
        this.unescapeJson = unescapeJson;
        this.errorHandleWay = errorHandleWay;
    }

    public static JsonPathFlatMapConfig of(ReadonlyConfig config, CatalogTable table) {
        // Validate required configurations
        if (!config.toConfig().hasPath(ARRAY_FLATTEN_FIELDS.key())) {
            throw new TransformException(
                    COLUMNS_MUST_NOT_EMPTY, "array_flatten_fields must not be empty");
        }
        if (!config.toConfig().hasPath(OUTPUT_FIELDS.key())) {
            throw new TransformException(COLUMNS_MUST_NOT_EMPTY, "output_fields must not be empty");
        }

        // Parse array flatten field configurations
        List<Map<String, Object>> arrayFlattenFieldMaps = config.get(ARRAY_FLATTEN_FIELDS);
        List<ArrayFlattenFieldConfig> arrayFlattenFields = new ArrayList<>();
        for (Map<String, Object> map : arrayFlattenFieldMaps) {
            String srcField = (String) map.get("src_field");
            String arrayPath = (String) map.get("array_path");
            String fieldKey = (String) map.get("field_key");

            if (StringUtils.isBlank(srcField)
                    || StringUtils.isBlank(arrayPath)
                    || StringUtils.isBlank(fieldKey)) {
                throw new TransformException(
                        COLUMNS_MUST_NOT_EMPTY,
                        "src_field, array_path, and field_key must not be empty");
            }

            if (!table.getTableSchema().contains(srcField)) {
                throw TransformCommonError.cannotFindInputFieldError("JsonPathFlatMap", srcField);
            }

            ErrorHandleWay columnErrorHandleWay =
                    Optional.ofNullable(
                                    (String)
                                            map.get(
                                                    TransformCommonOptions
                                                            .COLUMN_ERROR_HANDLE_WAY_OPTION
                                                            .key()))
                            .map(ErrorHandleWay::valueOf)
                            .orElse(null);

            arrayFlattenFields.add(
                    new ArrayFlattenFieldConfig(
                            srcField, arrayPath, fieldKey, columnErrorHandleWay));
        }

        // Parse output field configurations
        List<Map<String, Object>> outputFieldMaps = config.get(OUTPUT_FIELDS);
        List<OutputFieldConfig> outputFields = new ArrayList<>();
        for (Map<String, Object> map : outputFieldMaps) {
            String destField = (String) map.get("dest_field");
            String destType = (String) map.get("dest_type");
            String fromArray = (String) map.get("from_array");
            String srcField = (String) map.get("src_field");
            String itemPath = (String) map.get("item_path");
            String fillStrategyStr = (String) map.get("fill_strategy");

            if (StringUtils.isBlank(destField)
                    || StringUtils.isBlank(destType)
                    || StringUtils.isBlank(itemPath)) {
                throw new TransformException(
                        COLUMNS_MUST_NOT_EMPTY,
                        "dest_field, dest_type, and item_path must not be empty");
            }

            // Validation: when from_array is empty, src_field must be specified
            if (StringUtils.isBlank(fromArray) && StringUtils.isBlank(srcField)) {
                throw new TransformException(
                        COLUMNS_MUST_NOT_EMPTY,
                        "When from_array is empty, src_field must be specified for field: "
                                + destField);
            }

            SeaTunnelDataType<?> dataType =
                    SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType(
                            destField, destType);
            Column destColumn = PhysicalColumn.of(destField, dataType, 0, true, null, null);

            FillStrategy itemFillStrategy =
                    fillStrategyStr != null
                            ? FillStrategy.valueOf(fillStrategyStr.toUpperCase())
                            : null;

            // Parse column-level error handling method
            ErrorHandleWay columnErrorHandleWay =
                    Optional.ofNullable(
                                    (String)
                                            map.get(
                                                    TransformCommonOptions
                                                            .COLUMN_ERROR_HANDLE_WAY_OPTION
                                                            .key()))
                            .map(ErrorHandleWay::valueOf)
                            .orElse(null);

            outputFields.add(
                    new OutputFieldConfig(
                            destField,
                            destColumn,
                            fromArray,
                            srcField,
                            itemPath,
                            itemFillStrategy,
                            columnErrorHandleWay));
        }

        FieldMergeStrategy fieldMergeStrategy =
                FieldMergeStrategy.valueOf(config.get(FIELD_MERGE_STRATEGY).toUpperCase());
        List<String> preserveSourceFields = config.getOptional(PRESERVE_SOURCE_FIELDS).orElse(null);
        ArrayFlattenStrategy flattenStrategy =
                ArrayFlattenStrategy.valueOf(config.get(FLATTEN_STRATEGY).toUpperCase());
        FillStrategy fillStrategy = FillStrategy.valueOf(config.get(FILL_STRATEGY).toUpperCase());
        boolean unescapeJson = config.get(UNESCAPE_JSON);
        ErrorHandleWay errorHandleWay =
                config.get(TransformCommonOptions.ROW_ERROR_HANDLE_WAY_OPTION);

        return new JsonPathFlatMapConfig(
                arrayFlattenFields,
                outputFields,
                fieldMergeStrategy,
                preserveSourceFields,
                flattenStrategy,
                fillStrategy,
                unescapeJson,
                errorHandleWay);
    }

    @Getter
    public static class ArrayFlattenFieldConfig implements Serializable {
        private final String srcField;
        private final String arrayPath;
        private final String fieldKey;
        private final boolean isRootArray;
        private final ErrorHandleWay errorHandleWay;

        public ArrayFlattenFieldConfig(
                String srcField, String arrayPath, String fieldKey, ErrorHandleWay errorHandleWay) {
            this.srcField = srcField;
            this.arrayPath = arrayPath;
            this.fieldKey = fieldKey;
            this.errorHandleWay = errorHandleWay;
            // Determine if it's a root array: path "$" or "$[*]" indicates the entire JSON is an
            // array
            this.isRootArray = "$".equals(arrayPath) || "$[*]".equals(arrayPath);
        }

        public String getActualArrayPath() {
            return isRootArray ? "$[*]" : arrayPath;
        }
    }

    @Getter
    public static class OutputFieldConfig implements Serializable {
        private final String destField;
        private final Column destColumn;
        private final String fromArray;
        private final String srcField; // Added: specify JSON data source field
        private final String itemPath;
        private final FillStrategy fillStrategy;
        private final ErrorHandleWay errorHandleWay;

        public OutputFieldConfig(
                String destField,
                Column destColumn,
                String fromArray,
                String srcField,
                String itemPath,
                FillStrategy fillStrategy,
                ErrorHandleWay errorHandleWay) {
            this.destField = destField;
            this.destColumn = destColumn;
            this.fromArray = fromArray;
            this.srcField = srcField;
            this.itemPath = itemPath;
            this.fillStrategy = fillStrategy;
            this.errorHandleWay = errorHandleWay;
        }
    }

    public enum FieldMergeStrategy {
        APPEND_NEW, // Append new fields
        REPLACE_ALL, // Replace all
        KEEP_SOURCE_ORDER // Keep source field order
    }

    public enum ArrayFlattenStrategy {
        CARTESIAN_PRODUCT, // Cartesian product
        MAX_LENGTH_ALIGN, // Max length align
        PARALLEL_EXPAND // Parallel expand
    }

    public enum FillStrategy {
        NULL, // Fill with NULL
        REPEAT_LAST, // Repeat last value
        REPEAT_FIRST // Repeat first value
    }
}
