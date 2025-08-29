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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.transform.exception.ErrorDataTransformException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JsonPathFlatMapTransformTest {

    private CatalogTable catalogTable;
    private SeaTunnelRowType inputRowType;

    @BeforeEach
    public void setUp() {
        List<Column> columns = new ArrayList<>();
        columns.add(PhysicalColumn.of("id", BasicType.LONG_TYPE, 0, false, null, null));
        columns.add(PhysicalColumn.of("name", BasicType.STRING_TYPE, 0, true, null, null));
        columns.add(PhysicalColumn.of("data", BasicType.STRING_TYPE, 0, true, null, null));

        TableSchema tableSchema = TableSchema.builder().columns(columns).build();

        this.catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("test", "test", "test_table"),
                        tableSchema,
                        new HashMap<>(),
                        new ArrayList<>(),
                        "Test table for JsonPathFlatMap");

        this.inputRowType = catalogTable.getSeaTunnelRowType();
    }

    @Test
    public void testBasicArrayFlattening() {
        // Basic array flattening
        JsonPathFlatMapConfig config = createBasicConfig();
        JsonPathFlatMapTransform transform = new JsonPathFlatMapTransform(config, catalogTable);

        String jsonData =
                "{\"items\":[{\"name\":\"item1\",\"value\":100},{\"name\":\"item2\",\"value\":200}]}";
        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {1L, "test", jsonData});
        inputRow.setTableId(catalogTable.getTableId().toTablePath().getFullName());

        List<SeaTunnelRow> outputRows = transform.flatMap(inputRow);

        Assertions.assertNotNull(outputRows);
        Assertions.assertEquals(2, outputRows.size());

        SeaTunnelRow row1 = outputRows.get(0);
        Assertions.assertEquals(1L, row1.getField(0)); // id
        Assertions.assertEquals("test", row1.getField(1)); // name
        Assertions.assertEquals(jsonData, row1.getField(2)); // data
        Assertions.assertEquals("item1", row1.getField(3)); // item_name

        SeaTunnelRow row2 = outputRows.get(1);
        Assertions.assertEquals(1L, row2.getField(0)); // id
        Assertions.assertEquals("test", row2.getField(1)); // name
        Assertions.assertEquals(jsonData, row2.getField(2)); // data
        Assertions.assertEquals("item2", row2.getField(3)); // item_name
    }

    @Test
    public void testRootArrayFlattening() {
        // Root array flattening
        Map<String, Object> configMap = new HashMap<>();

        List<Map<String, Object>> arrayFlattenFields = new ArrayList<>();
        Map<String, Object> arrayField = new HashMap<>();
        arrayField.put("src_field", "data");
        arrayField.put("array_path", "$"); // Root array
        arrayField.put("field_key", "root_items");
        arrayFlattenFields.add(arrayField);
        configMap.put("array_flatten_fields", arrayFlattenFields);

        List<Map<String, Object>> outputFields = new ArrayList<>();

        Map<String, Object> outputField1 = new HashMap<>();
        outputField1.put("dest_field", "item_id");
        outputField1.put("dest_type", "bigint");
        outputField1.put("from_array", "root_items");
        outputField1.put("item_path", "$.id");
        outputFields.add(outputField1);

        Map<String, Object> outputField2 = new HashMap<>();
        outputField2.put("dest_field", "item_name");
        outputField2.put("dest_type", "string");
        outputField2.put("from_array", "root_items");
        outputField2.put("item_path", "$.name");
        outputFields.add(outputField2);

        configMap.put("output_fields", outputFields);
        configMap.put("field_merge_strategy", "APPEND_NEW");
        configMap.put("flatten_strategy", "MAX_LENGTH_ALIGN");
        configMap.put("fill_strategy", "NULL");

        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(configMap);
        JsonPathFlatMapConfig config = JsonPathFlatMapConfig.of(readonlyConfig, catalogTable);
        JsonPathFlatMapTransform transform = new JsonPathFlatMapTransform(config, catalogTable);

        String jsonData =
                "[{\"id\":1,\"name\":\"item1\"},{\"id\":2,\"name\":\"item2\"},{\"id\":3,\"name\":\"item3\"}]";
        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {1L, "test", jsonData});
        inputRow.setTableId(catalogTable.getTableId().toTablePath().getFullName());

        List<SeaTunnelRow> outputRows = transform.flatMap(inputRow);

        Assertions.assertNotNull(outputRows);
        Assertions.assertEquals(3, outputRows.size());

        for (int i = 0; i < 3; i++) {
            SeaTunnelRow row = outputRows.get(i);
            Assertions.assertEquals(1L, row.getField(0)); // id (source field)
            Assertions.assertEquals("test", row.getField(1)); // name (source field)
            Assertions.assertEquals(jsonData, row.getField(2)); // data (source field)
            Assertions.assertEquals(
                    (long) (i + 1), row.getField(3)); // item_id (extracted from root array)
            Assertions.assertEquals(
                    "item" + (i + 1), row.getField(4)); // item_name (extracted from root array)
        }
    }

    @Test
    public void testCartesianProductStrategy() {
        // Cartesian product strategy
        JsonPathFlatMapConfig config = createCartesianProductConfig();
        JsonPathFlatMapTransform transform = new JsonPathFlatMapTransform(config, catalogTable);

        String jsonData =
                "{\"products\":[{\"name\":\"prod1\"},{\"name\":\"prod2\"}],\"categories\":[{\"name\":\"cat1\"},{\"name\":\"cat2\"}]}";
        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {1L, "test", jsonData});
        inputRow.setTableId(catalogTable.getTableId().toTablePath().getFullName());

        List<SeaTunnelRow> outputRows = transform.flatMap(inputRow);

        // Verify result - should have 2x2=4 rows
        Assertions.assertNotNull(outputRows);
        Assertions.assertEquals(4, outputRows.size());

        String[] expectedProducts = {"prod1", "prod2", "prod1", "prod2"};
        String[] expectedCategories = {"cat1", "cat1", "cat2", "cat2"};

        for (int i = 0; i < 4; i++) {
            SeaTunnelRow row = outputRows.get(i);
            Assertions.assertEquals(expectedProducts[i], row.getField(3)); // product_name
            Assertions.assertEquals(expectedCategories[i], row.getField(4)); // category_name
        }
    }

    @Test
    public void testMaxLengthAlignStrategy() {
        // Max length align strategy
        JsonPathFlatMapConfig config = createMaxLengthAlignConfig();
        JsonPathFlatMapTransform transform = new JsonPathFlatMapTransform(config, catalogTable);

        String jsonData =
                "{\"products\":[{\"name\":\"prod1\"},{\"name\":\"prod2\"},{\"name\":\"prod3\"}],\"categories\":[{\"name\":\"cat1\"},{\"name\":\"cat2\"}]}";
        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {1L, "test", jsonData});
        inputRow.setTableId(catalogTable.getTableId().toTablePath().getFullName());

        List<SeaTunnelRow> outputRows = transform.flatMap(inputRow);

        // Verify result - should expand by max length 3
        Assertions.assertNotNull(outputRows);
        Assertions.assertEquals(3, outputRows.size());

        String[] expectedProducts = {"prod1", "prod2", "prod3"};
        String[] expectedCategories = {
            "cat1", "cat2", null
        }; // Third should be null (according to fill strategy)

        for (int i = 0; i < 3; i++) {
            SeaTunnelRow row = outputRows.get(i);
            Assertions.assertEquals(expectedProducts[i], row.getField(3)); // product_name
            Assertions.assertEquals(expectedCategories[i], row.getField(4)); // category_name
        }
    }

    @Test
    public void testParallelExpandStrategy() {
        // Parallel expand strategy
        JsonPathFlatMapConfig config = createParallelExpandConfig();
        JsonPathFlatMapTransform transform = new JsonPathFlatMapTransform(config, catalogTable);

        String jsonData =
                "{\"products\":[{\"name\":\"prod1\"},{\"name\":\"prod2\"},{\"name\":\"prod3\"}],\"categories\":[{\"name\":\"cat1\"},{\"name\":\"cat2\"}]}";
        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {1L, "test", jsonData});
        inputRow.setTableId(catalogTable.getTableId().toTablePath().getFullName());

        List<SeaTunnelRow> outputRows = transform.flatMap(inputRow);

        // Verify result - should expand by min length 2
        Assertions.assertNotNull(outputRows);
        Assertions.assertEquals(2, outputRows.size());

        String[] expectedProducts = {"prod1", "prod2"};
        String[] expectedCategories = {"cat1", "cat2"};

        for (int i = 0; i < 2; i++) {
            SeaTunnelRow row = outputRows.get(i);
            Assertions.assertEquals(expectedProducts[i], row.getField(3)); // product_name
            Assertions.assertEquals(expectedCategories[i], row.getField(4)); // category_name
        }
    }

    @Test
    public void testEmptyArrayHandling() {
        // Empty array handling
        JsonPathFlatMapConfig config = createBasicConfig();
        JsonPathFlatMapTransform transform = new JsonPathFlatMapTransform(config, catalogTable);
        String jsonData = "{\"items\":[]}";
        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {1L, "test", jsonData});
        inputRow.setTableId(catalogTable.getTableId().toTablePath().getFullName());
        List<SeaTunnelRow> outputRows = transform.flatMap(inputRow);

        // Verify result - empty array should result in NULL fields, as JsonPath cannot extract and
        // won't throw exception
        Assertions.assertNotNull(outputRows);
        Assertions.assertNull(outputRows.get(0).getField(3));
    }

    @Test
    public void testNullDataHandling() {
        // Null data handling
        JsonPathFlatMapConfig config = createBasicConfig();
        JsonPathFlatMapTransform transform = new JsonPathFlatMapTransform(config, catalogTable);

        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {1L, "test", null});
        inputRow.setTableId(catalogTable.getTableId().toTablePath().getFullName());

        List<SeaTunnelRow> outputRows = transform.flatMap(inputRow);

        // Verify result - null data should return empty
        Assertions.assertNotNull(outputRows);
        Assertions.assertNull(outputRows.get(0).getField(3));
    }

    @Test
    public void testUnescapeJsonFeature() {
        // JSON unescape feature
        Map<String, Object> configMap = new HashMap<>();

        List<Map<String, Object>> arrayFlattenFields = new ArrayList<>();
        Map<String, Object> arrayField = new HashMap<>();
        arrayField.put("src_field", "data");
        arrayField.put("array_path", "$.products");
        arrayField.put("field_key", "products_array");
        arrayFlattenFields.add(arrayField);
        configMap.put("array_flatten_fields", arrayFlattenFields);

        List<Map<String, Object>> outputFields = new ArrayList<>();
        Map<String, Object> outputField1 = new HashMap<>();
        outputField1.put("dest_field", "product_name");
        outputField1.put("dest_type", "string");
        outputField1.put("from_array", "products_array");
        outputField1.put("item_path", "$.name");
        outputFields.add(outputField1);

        Map<String, Object> outputField2 = new HashMap<>();
        outputField2.put("dest_field", "product_price");
        outputField2.put("dest_type", "decimal(10,2)");
        outputField2.put("from_array", "products_array");
        outputField2.put("item_path", "$.price");
        outputFields.add(outputField2);
        configMap.put("output_fields", outputFields);

        configMap.put("field_merge_strategy", "APPEND_NEW");
        configMap.put("flatten_strategy", "MAX_LENGTH_ALIGN");
        configMap.put("fill_strategy", "NULL");
        configMap.put("unescape_json", true); // Enable JSON unescape

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        JsonPathFlatMapConfig transformConfig = JsonPathFlatMapConfig.of(config, catalogTable);
        JsonPathFlatMapTransform transform =
                new JsonPathFlatMapTransform(transformConfig, catalogTable);

        // Input data: escaped JSON string
        String escapedJson =
                "{\\\"products\\\":[{\\\"name\\\":\\\"prod1\\\",\\\"price\\\":100},{\\\"name\\\":\\\"prod2\\\",\\\"price\\\":200}]}";
        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {1L, "test", escapedJson});
        inputRow.setTableId(catalogTable.getTableId().toTablePath().getFullName());

        List<SeaTunnelRow> outputRows = transform.flatMap(inputRow);

        Assertions.assertNotNull(outputRows);
        Assertions.assertEquals(2, outputRows.size());

        SeaTunnelRow row1 = outputRows.get(0);
        Assertions.assertEquals("prod1", row1.getField(3)); // product_name
        Assertions.assertEquals(new BigDecimal("100"), row1.getField(4)); // product_price

        SeaTunnelRow row2 = outputRows.get(1);
        Assertions.assertEquals("prod2", row2.getField(3)); // product_name
        Assertions.assertEquals(new BigDecimal("200"), row2.getField(4)); // product_price
    }

    @Test
    public void testUnescapeJsonDisabled() {
        // JSON unescape disabled
        Map<String, Object> configMap = new HashMap<>();

        // Array flatten field configuration
        List<Map<String, Object>> arrayFlattenFields = new ArrayList<>();
        Map<String, Object> arrayField = new HashMap<>();
        arrayField.put("src_field", "data");
        arrayField.put("array_path", "$.products");
        arrayField.put("field_key", "products_array");
        arrayFlattenFields.add(arrayField);
        configMap.put("array_flatten_fields", arrayFlattenFields);

        // Output field configuration
        List<Map<String, Object>> outputFields = new ArrayList<>();
        Map<String, Object> outputField1 = new HashMap<>();
        outputField1.put("dest_field", "product_name");
        outputField1.put("dest_type", "string");
        outputField1.put("from_array", "products_array");
        outputField1.put("item_path", "$.name");
        outputFields.add(outputField1);
        configMap.put("output_fields", outputFields);

        // Other configurations
        configMap.put("field_merge_strategy", "APPEND_NEW");
        configMap.put("flatten_strategy", "MAX_LENGTH_ALIGN");
        configMap.put("fill_strategy", "NULL");
        configMap.put("unescape_json", false); // Disable JSON unescape
        configMap.put("row_error_handle_way", "FAIL"); // Set error handling to fail

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        JsonPathFlatMapConfig transformConfig = JsonPathFlatMapConfig.of(config, catalogTable);
        JsonPathFlatMapTransform transform =
                new JsonPathFlatMapTransform(transformConfig, catalogTable);

        // Input data: escaped JSON string
        String escapedJson = "{\\\"products\\\":[{\\\"name\\\":\\\"prod1\\\",\\\"price\\\":100}]}";
        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {1L, "test", escapedJson});
        inputRow.setTableId(catalogTable.getTableId().toTablePath().getFullName());

        // Since there's no unescape, JsonPath parsing will fail and throw exception
        Assertions.assertThrows(
                ErrorDataTransformException.class,
                () -> transform.flatMap(inputRow),
                "Should throw ErrorDataTransformException because escaped JSON cannot be parsed by JsonPath");
    }

    @Test
    public void testComplexNestedJsonUnescape() {
        // Complex nested JSON unescape
        Map<String, Object> configMap = new HashMap<>();

        // Array flatten field configuration
        List<Map<String, Object>> arrayFlattenFields = new ArrayList<>();
        Map<String, Object> arrayField = new HashMap<>();
        arrayField.put("src_field", "data");
        arrayField.put("array_path", "$.products");
        arrayField.put("field_key", "products_array");
        arrayFlattenFields.add(arrayField);
        configMap.put("array_flatten_fields", arrayFlattenFields);

        // Output field configuration
        List<Map<String, Object>> outputFields = new ArrayList<>();
        Map<String, Object> outputField1 = new HashMap<>();
        outputField1.put("dest_field", "product_name");
        outputField1.put("dest_type", "string");
        outputField1.put("from_array", "products_array");
        outputField1.put("item_path", "$.name");
        outputFields.add(outputField1);

        Map<String, Object> outputField2 = new HashMap<>();
        outputField2.put("dest_field", "product_price");
        outputField2.put("dest_type", "decimal(10,2)");
        outputField2.put("from_array", "products_array");
        outputField2.put("item_path", "$.price");
        outputFields.add(outputField2);
        configMap.put("output_fields", outputFields);

        // Other configurations
        configMap.put("field_merge_strategy", "APPEND_NEW");
        configMap.put("flatten_strategy", "MAX_LENGTH_ALIGN");
        configMap.put("fill_strategy", "NULL");
        configMap.put("unescape_json", true); // Enable JSON unescape

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        JsonPathFlatMapConfig transformConfig = JsonPathFlatMapConfig.of(config, catalogTable);
        JsonPathFlatMapTransform transform =
                new JsonPathFlatMapTransform(transformConfig, catalogTable);

        // Input data: complex nested escaped JSON string
        String complexEscapedJson =
                "{\\\"products\\\":[{\\\"name\\\":\\\"{\\\\\\\"name\\\\\\\":\\\\\\\"\\\\\\\",\\\\\\\"preTaskCode\\\\\\\":18775175828264,\\\\\\\"preTaskVersion\\\\\\\":0,\\\\\\\"postTaskCode\\\\\\\":18775397135793,\\\\\\\"postTaskVersion\\\\\\\":0,\\\\\\\"conditionType\\\\\\\":\\\\\\\"NONE\\\\\\\",\\\\\\\"conditionParams\\\\\\\":{}}\\\",\\\"price\\\":100}]}";
        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {1L, "test", complexEscapedJson});
        inputRow.setTableId(catalogTable.getTableId().toTablePath().getFullName());

        // Execute transformation
        List<SeaTunnelRow> outputRows = transform.flatMap(inputRow);

        // Verify result
        Assertions.assertNotNull(outputRows);
        Assertions.assertEquals(1, outputRows.size());

        SeaTunnelRow row1 = outputRows.get(0);
        // Verify complex nested name field is correctly extracted
        String expectedName =
                "{\"name\":\"\",\"preTaskCode\":18775175828264,\"preTaskVersion\":0,\"postTaskCode\":18775397135793,\"postTaskVersion\":0,\"conditionType\":\"NONE\",\"conditionParams\":{}}";
        Assertions.assertEquals(expectedName, row1.getField(3)); // product_name
        Assertions.assertEquals(new BigDecimal("100"), row1.getField(4)); // product_price
    }

    @Test
    public void testUnescapeJsonDisabledThrowsException() {
        // JSON unescape disabled exception handling
        Map<String, Object> configMap = new HashMap<>();

        // Array flatten field configuration
        List<Map<String, Object>> arrayFlattenFields = new ArrayList<>();
        Map<String, Object> arrayField = new HashMap<>();
        arrayField.put("src_field", "data");
        arrayField.put("array_path", "$.products");
        arrayField.put("field_key", "products_array");
        arrayFlattenFields.add(arrayField);
        configMap.put("array_flatten_fields", arrayFlattenFields);

        // Output field configuration
        List<Map<String, Object>> outputFields = new ArrayList<>();
        Map<String, Object> outputField1 = new HashMap<>();
        outputField1.put("dest_field", "product_name");
        outputField1.put("dest_type", "string");
        outputField1.put("from_array", "products_array");
        outputField1.put("item_path", "$.name");
        outputFields.add(outputField1);
        configMap.put("output_fields", outputFields);

        // Other configurations
        configMap.put("field_merge_strategy", "APPEND_NEW");
        configMap.put("flatten_strategy", "MAX_LENGTH_ALIGN");
        configMap.put("fill_strategy", "NULL");
        configMap.put("unescape_json", false); // Disable JSON unescape
        configMap.put(
                "row_error_handle_way", "FAIL"); // Set error handling to fail (throw exception)

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        JsonPathFlatMapConfig transformConfig = JsonPathFlatMapConfig.of(config, catalogTable);
        JsonPathFlatMapTransform transform =
                new JsonPathFlatMapTransform(transformConfig, catalogTable);

        // Input data: escaped JSON string
        String escapedJson = "{\\\"products\\\":[{\\\"name\\\":\\\"prod1\\\",\\\"price\\\":100}]}";
        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {1L, "test", escapedJson});
        inputRow.setTableId(catalogTable.getTableId().toTablePath().getFullName());

        // Verify exception is thrown
        Assertions.assertThrows(
                ErrorDataTransformException.class,
                () -> transform.flatMap(inputRow),
                "Should throw ErrorDataTransformException because escaped JSON cannot be parsed by JsonPath");
    }

    @Test
    public void testJsonUnescapeErrorCode() {
        // JSON unescape error code validation
        Map<String, Object> configMap = new HashMap<>();

        // Array flatten field configuration
        List<Map<String, Object>> arrayFlattenFields = new ArrayList<>();
        Map<String, Object> arrayField = new HashMap<>();
        arrayField.put("src_field", "data");
        arrayField.put("array_path", "$.products");
        arrayField.put("field_key", "products_array");
        arrayFlattenFields.add(arrayField);
        configMap.put("array_flatten_fields", arrayFlattenFields);

        // Output field configuration
        List<Map<String, Object>> outputFields = new ArrayList<>();
        Map<String, Object> outputField1 = new HashMap<>();
        outputField1.put("dest_field", "product_name");
        outputField1.put("dest_type", "string");
        outputField1.put("from_array", "products_array");
        outputField1.put("item_path", "$.name");
        outputFields.add(outputField1);
        configMap.put("output_fields", outputFields);

        // Other configurations
        configMap.put("field_merge_strategy", "APPEND_NEW");
        configMap.put("flatten_strategy", "MAX_LENGTH_ALIGN");
        configMap.put("fill_strategy", "NULL");
        configMap.put("unescape_json", true); // Enable JSON unescape
        configMap.put("row_error_handle_way", "FAIL"); // Set to fail mode

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        JsonPathFlatMapConfig transformConfig = JsonPathFlatMapConfig.of(config, catalogTable);
        JsonPathFlatMapTransform transform =
                new JsonPathFlatMapTransform(transformConfig, catalogTable);

        // Input data: intentionally use invalid escape string to trigger unescape error
        String invalidEscapedJson = "\\invalid\\escape\\sequence";
        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {1L, "test", invalidEscapedJson});
        inputRow.setTableId(catalogTable.getTableId().toTablePath().getFullName());

        // Verify correct exception and error code are thrown
        ErrorDataTransformException exception =
                Assertions.assertThrows(
                        ErrorDataTransformException.class,
                        () -> transform.flatMap(inputRow),
                        "Should throw ErrorDataTransformException because JSON unescape failed or JsonPath parsing failed");

        // Verify error code
        Assertions.assertTrue(
                exception.getMessage().contains("JSONPATH_FLATMAP_ERROR_CODE"),
                "Exception message should contain JsonPathFlatMap specific error code");
    }

    /** Create basic configuration map */
    private Map<String, Object> createBasicConfigMap() {
        Map<String, Object> configMap = new HashMap<>();

        // Array flatten field configuration
        List<Map<String, Object>> arrayFlattenFields = new ArrayList<>();
        Map<String, Object> arrayField = new HashMap<>();
        arrayField.put("src_field", "data");
        arrayField.put("array_path", "$.products");
        arrayField.put("field_key", "products_array");
        arrayFlattenFields.add(arrayField);
        configMap.put("array_flatten_fields", arrayFlattenFields);

        // Output field configuration
        List<Map<String, Object>> outputFields = new ArrayList<>();
        Map<String, Object> outputField1 = new HashMap<>();
        outputField1.put("dest_field", "product_name");
        outputField1.put("dest_type", "string");
        outputField1.put("from_array", "products_array");
        outputField1.put("item_path", "$.name");
        outputFields.add(outputField1);
        configMap.put("output_fields", outputFields);

        // Other configurations
        configMap.put("field_merge_strategy", "APPEND_NEW");
        configMap.put("flatten_strategy", "MAX_LENGTH_ALIGN");
        configMap.put("fill_strategy", "NULL");

        return configMap;
    }

    // Helper method: create array flatten field configuration
    private Map<String, Object> createArrayFlattenField(
            String srcField, String arrayPath, String fieldKey, String errorHandleWay) {
        Map<String, Object> field = new HashMap<>();
        field.put("src_field", srcField);
        field.put("array_path", arrayPath);
        field.put("field_key", fieldKey);
        if (errorHandleWay != null) {
            field.put("column_error_handle_way", errorHandleWay);
        }
        return field;
    }

    // Helper method: create output field configuration
    private Map<String, Object> createOutputField(
            String destField, String destType, String fromArray, String itemPath) {
        Map<String, Object> field = new HashMap<>();
        field.put("dest_field", destField);
        field.put("dest_type", destType);
        field.put("from_array", fromArray);
        field.put("item_path", itemPath);
        return field;
    }

    private JsonPathFlatMapTransform createTransform(Map<String, Object> configMap) {
        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        JsonPathFlatMapConfig transformConfig = JsonPathFlatMapConfig.of(config, catalogTable);
        return new JsonPathFlatMapTransform(transformConfig, catalogTable);
    }

    // Helper method: create output field configuration with error handling
    private Map<String, Object> createOutputFieldWithErrorHandling(
            String destField,
            String destType,
            String fromArray,
            String itemPath,
            String errorHandleWay) {
        Map<String, Object> field = new HashMap<>();
        field.put("dest_field", destField);
        field.put("dest_type", destType);
        field.put("from_array", fromArray);
        field.put("item_path", itemPath);
        if (errorHandleWay != null) {
            field.put("column_error_handle_way", errorHandleWay);
        }
        return field;
    }

    @Test
    public void testColumnErrorHandleWaySkip() {
        // Column-level error handling: SKIP strategy

        Map<String, Object> configMap = new HashMap<>();

        // Configure array flatten fields
        List<Map<String, Object>> arrayFlattenFields =
                Arrays.asList(
                        createArrayFlattenField(
                                "data",
                                "$.products[*]",
                                "products",
                                null) // Correct path, use global error handling
                        );
        configMap.put("array_flatten_fields", arrayFlattenFields);

        // Configure output fields: one with error JsonPath, one with correct JsonPath
        List<Map<String, Object>> outputFields =
                Arrays.asList(
                        createOutputFieldWithErrorHandling(
                                "json_data_f1",
                                "string",
                                "products",
                                "$.invalid_field",
                                "SKIP"), // Error path, column-level SKIP
                        createOutputField(
                                "json_data_f2",
                                "string",
                                "products",
                                "$.name") // Correct path, use global error handling
                        );
        configMap.put("output_fields", outputFields);

        // Row-level error handling set to FAIL
        configMap.put("row_error_handle_way", "FAIL");
        configMap.put("field_merge_strategy", "APPEND_NEW");
        configMap.put("flatten_strategy", "MAX_LENGTH_ALIGN");
        configMap.put("fill_strategy", "NULL");

        JsonPathFlatMapTransform transform = createTransform(configMap);

        // Create test data
        SeaTunnelRow inputRow =
                new SeaTunnelRow(
                        new Object[] {
                            1L, "test", "{\"products\":[{\"name\":\"prod1\"},{\"name\":\"prod2\"}]}"
                        });
        inputRow.setTableId(catalogTable.getTableId().toTablePath().getFullName());

        // Execute transformation - should succeed because error field is skipped
        List<SeaTunnelRow> result = transform.flatMap(inputRow);

        // Verify result: error field is skipped (filled with null), correct field processed
        // normally
        Assertions.assertEquals(2, result.size());
        Assertions.assertEquals(1L, result.get(0).getField(0)); // id
        Assertions.assertEquals("test", result.get(0).getField(1)); // name
        Assertions.assertNull(result.get(0).getField(3)); // json_data_f1 skipped, filled with null
        Assertions.assertEquals(
                "prod1", result.get(0).getField(4)); // json_data_f2 processed normally

        Assertions.assertEquals(1L, result.get(1).getField(0)); // id
        Assertions.assertEquals("test", result.get(1).getField(1)); // name
        Assertions.assertNull(result.get(1).getField(3)); // json_data_f1 skipped, filled with null
        Assertions.assertEquals(
                "prod2", result.get(1).getField(4)); // json_data_f2 processed normally
    }

    @Test
    public void testColumnErrorHandleWaySkipRow() {
        // Column-level error handling: SKIP_ROW strategy

        Map<String, Object> configMap = new HashMap<>();

        // Configure array flatten fields
        List<Map<String, Object>> arrayFlattenFields =
                Arrays.asList(createArrayFlattenField("data", "$.products[*]", "products", null));
        configMap.put("array_flatten_fields", arrayFlattenFields);

        // Configure output fields: one with error JsonPath set to SKIP_ROW, one with correct
        // JsonPath
        List<Map<String, Object>> outputFields =
                Arrays.asList(
                        createOutputFieldWithErrorHandling(
                                "json_data_f1",
                                "string",
                                "products",
                                "$.invalid_field",
                                "SKIP_ROW"), // Error path, column-level SKIP_ROW
                        createOutputField(
                                "json_data_f2", "string", "products", "$.name") // Correct path
                        );
        configMap.put("output_fields", outputFields);

        // Row-level error handling set to FAIL
        configMap.put("row_error_handle_way", "FAIL");
        configMap.put("field_merge_strategy", "APPEND_NEW");
        configMap.put("flatten_strategy", "MAX_LENGTH_ALIGN");
        configMap.put("fill_strategy", "NULL");

        JsonPathFlatMapTransform transform = createTransform(configMap);

        // Create test data
        SeaTunnelRow inputRow =
                new SeaTunnelRow(
                        new Object[] {
                            1L, "test", "{\"products\":[{\"name\":\"prod1\"},{\"name\":\"prod2\"}]}"
                        });
        inputRow.setTableId(catalogTable.getTableId().toTablePath().getFullName());

        // Execute transformation - should return empty list because entire row is skipped when
        // field error occurs
        List<SeaTunnelRow> result = transform.flatMap(inputRow);

        // Verify result: entire row is skipped
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void testColumnErrorHandleWayFail() {
        // Column-level error handling: FAIL strategy

        Map<String, Object> configMap = new HashMap<>();

        List<Map<String, Object>> arrayFlattenFields =
                Arrays.asList(createArrayFlattenField("data", "$.products[*]", "products", null));
        configMap.put("array_flatten_fields", arrayFlattenFields);

        List<Map<String, Object>> outputFields =
                Arrays.asList(
                        createOutputFieldWithErrorHandling(
                                "json_data_f1",
                                "int", // Use int type to force conversion error
                                "products",
                                "$.name", // Valid path but wrong type (name is string, not int)
                                "FAIL"), // Column-level FAIL
                        createOutputField(
                                "json_data_f2", "string", "products", "$.name") // Correct path
                        );
        configMap.put("output_fields", outputFields);

        // Row-level error handling set to SKIP, but column-level FAIL should take priority
        configMap.put("row_error_handle_way", "SKIP");
        configMap.put("field_merge_strategy", "APPEND_NEW");
        configMap.put("flatten_strategy", "MAX_LENGTH_ALIGN");
        configMap.put("fill_strategy", "NULL");

        JsonPathFlatMapTransform transform = createTransform(configMap);

        SeaTunnelRow inputRow =
                new SeaTunnelRow(
                        new Object[] {
                            1L, "test", "{\"products\":[{\"name\":\"prod1\"},{\"name\":\"prod2\"}]}"
                        });
        inputRow.setTableId(catalogTable.getTableId().toTablePath().getFullName());

        ErrorDataTransformException exception =
                Assertions.assertThrows(
                        ErrorDataTransformException.class,
                        () -> transform.flatMap(inputRow),
                        "Should throw ErrorDataTransformException because column-level error handling is set to FAIL");

        Assertions.assertTrue(
                exception.getMessage().contains("JsonPath FlatMap transform error"),
                "Exception message should contain JsonPath FlatMap error information");
    }

    @Test
    public void testMixedErrorHandling() {
        // Mixed error handling strategies
        Map<String, Object> configMap = new HashMap<>();

        List<Map<String, Object>> arrayFlattenFields =
                Arrays.asList(createArrayFlattenField("data", "$.products[*]", "products", null));
        configMap.put("array_flatten_fields", arrayFlattenFields);

        List<Map<String, Object>> outputFields =
                Arrays.asList(
                        createOutputFieldWithErrorHandling(
                                "json_data_f1",
                                "string",
                                "products",
                                "$.invalid_field1",
                                "SKIP"), // Error path, column-level SKIP
                        createOutputField(
                                "json_data_f2",
                                "string",
                                "products",
                                "$.name"), // Correct path, use global error handling
                        createOutputFieldWithErrorHandling(
                                "json_data_f3",
                                "string",
                                "products",
                                "$.invalid_field2",
                                "SKIP") // Error path, column-level SKIP
                        );
        configMap.put("output_fields", outputFields);

        configMap.put("row_error_handle_way", "FAIL");
        configMap.put("field_merge_strategy", "APPEND_NEW");
        configMap.put("flatten_strategy", "MAX_LENGTH_ALIGN");
        configMap.put("fill_strategy", "NULL");

        JsonPathFlatMapTransform transform = createTransform(configMap);

        SeaTunnelRow inputRow =
                new SeaTunnelRow(
                        new Object[] {
                            1L, "test", "{\"products\":[{\"name\":\"prod1\"},{\"name\":\"prod2\"}]}"
                        });
        inputRow.setTableId(catalogTable.getTableId().toTablePath().getFullName());

        List<SeaTunnelRow> result = transform.flatMap(inputRow);

        // Verify result: error fields are skipped (filled with null), correct field processed
        // normally
        // Field order: id(0), name(1), data(2), json_data_f1(3), json_data_f2(4), json_data_f3(5)
        Assertions.assertEquals(2, result.size());
        Assertions.assertEquals(1L, result.get(0).getField(0)); // id
        Assertions.assertEquals("test", result.get(0).getField(1)); // name
        Assertions.assertEquals(
                "{\"products\":[{\"name\":\"prod1\"},{\"name\":\"prod2\"}]}",
                result.get(0).getField(2)); // data (source field)
        Assertions.assertNull(result.get(0).getField(3)); // json_data_f1 skipped, filled with null
        Assertions.assertEquals(
                "prod1", result.get(0).getField(4)); // json_data_f2 processed normally
        Assertions.assertNull(result.get(0).getField(5)); // json_data_f3 skipped, filled with null

        Assertions.assertEquals(1L, result.get(1).getField(0)); // id
        Assertions.assertEquals("test", result.get(1).getField(1)); // name
        Assertions.assertEquals(
                "{\"products\":[{\"name\":\"prod1\"},{\"name\":\"prod2\"}]}",
                result.get(1).getField(2)); // data (source field)
        Assertions.assertNull(result.get(1).getField(3)); // json_data_f1 skipped, filled with null
        Assertions.assertEquals(
                "prod2", result.get(1).getField(4)); // json_data_f2 processed normally
        Assertions.assertNull(result.get(1).getField(5)); // json_data_f3 skipped, filled with null
    }

    @Test
    public void testArrayFieldErrorHandling() {
        // Array field error handling
        Map<String, Object> configMap = new HashMap<>();

        List<Map<String, Object>> arrayFlattenFields =
                Arrays.asList(
                        createArrayFlattenFieldWithErrorHandling(
                                "data", "$.invalid_array[*]", "products", "SKIP"));
        configMap.put("array_flatten_fields", arrayFlattenFields);

        List<Map<String, Object>> outputFields =
                Arrays.asList(createOutputField("json_data_f1", "string", "products", "$.name"));
        configMap.put("output_fields", outputFields);

        // Row-level error handling set to FAIL
        configMap.put("row_error_handle_way", "FAIL");
        configMap.put("field_merge_strategy", "APPEND_NEW");
        configMap.put("flatten_strategy", "MAX_LENGTH_ALIGN");
        configMap.put("fill_strategy", "NULL");

        JsonPathFlatMapTransform transform = createTransform(configMap);

        SeaTunnelRow inputRow =
                new SeaTunnelRow(
                        new Object[] {
                            1L, "test", "{\"products\":[{\"name\":\"prod1\"},{\"name\":\"prod2\"}]}"
                        });
        inputRow.setTableId(catalogTable.getTableId().toTablePath().getFullName());

        List<SeaTunnelRow> result = transform.flatMap(inputRow);

        // Verify result: array field error is skipped, returns single row result
        // Field order: id(0), name(1), data(2), json_data_f1(3)
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(1L, result.get(0).getField(0)); // id
        Assertions.assertEquals("test", result.get(0).getField(1)); // name
        Assertions.assertEquals(
                "{\"products\":[{\"name\":\"prod1\"},{\"name\":\"prod2\"}]}",
                result.get(0).getField(2)); // data (source field)
        // Since array field error is skipped, output field cannot be extracted, should be null
        Assertions.assertNull(result.get(0).getField(3)); // json_data_f1
    }

    @Test
    public void testArrayFieldErrorHandlingSkipRow() {
        // Array field error handling: SKIP_ROW strategy
        Map<String, Object> configMap = new HashMap<>();

        // Use error JsonPath expression, set column-level error handling to SKIP_ROW
        List<Map<String, Object>> arrayFlattenFields =
                Arrays.asList(
                        createArrayFlattenFieldWithErrorHandling(
                                "data", "$.invalid_array[*]", "products", "SKIP_ROW"));
        configMap.put("array_flatten_fields", arrayFlattenFields);

        List<Map<String, Object>> outputFields =
                Arrays.asList(createOutputField("json_data_f1", "string", "products", "$.name"));
        configMap.put("output_fields", outputFields);

        // Row-level error handling set to FAIL
        configMap.put("row_error_handle_way", "FAIL");
        configMap.put("field_merge_strategy", "APPEND_NEW");
        configMap.put("flatten_strategy", "MAX_LENGTH_ALIGN");
        configMap.put("fill_strategy", "NULL");

        JsonPathFlatMapTransform transform = createTransform(configMap);

        // Create test data
        SeaTunnelRow inputRow =
                new SeaTunnelRow(
                        new Object[] {
                            1L, "test", "{\"products\":[{\"name\":\"prod1\"},{\"name\":\"prod2\"}]}"
                        });
        inputRow.setTableId(catalogTable.getTableId().toTablePath().getFullName());

        // Execute transformation - array field error causes entire row to be skipped, should return
        // empty result
        List<SeaTunnelRow> result = transform.flatMap(inputRow);
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void testDataConversionErrorHandling() {
        // Test data conversion exception error handling: type conversion error
        Map<String, Object> configMap = new HashMap<>();

        // Configure array flatten fields
        List<Map<String, Object>> arrayFlattenFields =
                Arrays.asList(createArrayFlattenField("data", "$.products[*]", "products", null));
        configMap.put("array_flatten_fields", arrayFlattenFields);

        // Configure output fields: try to convert string to number type, set column-level error
        // handling to SKIP
        List<Map<String, Object>> outputFields =
                Arrays.asList(
                        createOutputFieldWithErrorHandling(
                                "json_data_f1",
                                "int",
                                "products",
                                "$.name",
                                "SKIP"), // String to number conversion, will error
                        createOutputField(
                                "json_data_f2",
                                "string",
                                "products",
                                "$.name") // Correct string field
                        );
        configMap.put("output_fields", outputFields);

        // Row-level error handling set to FAIL
        configMap.put("row_error_handle_way", "FAIL");
        configMap.put("field_merge_strategy", "APPEND_NEW");
        configMap.put("flatten_strategy", "MAX_LENGTH_ALIGN");
        configMap.put("fill_strategy", "NULL");

        JsonPathFlatMapTransform transform = createTransform(configMap);

        // Create test data
        SeaTunnelRow inputRow =
                new SeaTunnelRow(
                        new Object[] {
                            1L, "test", "{\"products\":[{\"name\":\"prod1\"},{\"name\":\"prod2\"}]}"
                        });
        inputRow.setTableId(catalogTable.getTableId().toTablePath().getFullName());

        // Execute transformation - data conversion error is skipped, correct field processed
        // normally
        List<SeaTunnelRow> result = transform.flatMap(inputRow);

        // Verify result: data conversion error is skipped (filled with null), correct field
        // processed normally
        Assertions.assertEquals(2, result.size());
        Assertions.assertEquals(1L, result.get(0).getField(0)); // id
        Assertions.assertEquals("test", result.get(0).getField(1)); // name
        Assertions.assertEquals(
                "{\"products\":[{\"name\":\"prod1\"},{\"name\":\"prod2\"}]}",
                result.get(0).getField(2)); // data (source field)
        Assertions.assertNull(
                result.get(0)
                        .getField(
                                3)); // json_data_f1 conversion error, skipped and filled with null
        Assertions.assertEquals(
                "prod1", result.get(0).getField(4)); // json_data_f2 processed normally

        Assertions.assertEquals(1L, result.get(1).getField(0)); // id
        Assertions.assertEquals("test", result.get(1).getField(1)); // name
        Assertions.assertEquals(
                "{\"products\":[{\"name\":\"prod1\"},{\"name\":\"prod2\"}]}",
                result.get(1).getField(2)); // data (source field)
        Assertions.assertNull(
                result.get(1)
                        .getField(
                                3)); // json_data_f1 conversion error, skipped and filled with null
        Assertions.assertEquals(
                "prod2", result.get(1).getField(4)); // json_data_f2 processed normally
    }

    @Test
    public void testArrayWithParentAndGrandparentFields() {
        // Test array fields + parent/grandparent node field extraction
        Map<String, Object> configMap = new HashMap<>();

        List<Map<String, Object>> arrayFlattenFields =
                Arrays.asList(
                        createArrayFlattenField(
                                "data",
                                "$.company.departments[*]",
                                "dept",
                                null) // Department array
                        );
        configMap.put("array_flatten_fields", arrayFlattenFields);

        List<Map<String, Object>> outputFields =
                Arrays.asList(
                        // Array element fields
                        createOutputField("dept_name", "string", "dept", "$.name"),
                        createOutputField("dept_budget", "int", "dept", "$.budget"),
                        // Parent node fields (company) - use src_field for single field extraction
                        createSingleFieldOutput("company_name", "string", "data", "$.company.name"),
                        createSingleFieldOutput(
                                "company_founded", "int", "data", "$.company.founded"),
                        // Grandparent node fields (root node) - use src_field for single field
                        // extraction
                        createSingleFieldOutput("organization_type", "string", "data", "$.type"),
                        createSingleFieldOutput(
                                "organization_country", "string", "data", "$.country"));
        configMap.put("output_fields", outputFields);

        configMap.put("row_error_handle_way", "FAIL");
        configMap.put("field_merge_strategy", "APPEND_NEW");
        configMap.put("flatten_strategy", "MAX_LENGTH_ALIGN");
        configMap.put("fill_strategy", "NULL");

        JsonPathFlatMapTransform transform = createTransform(configMap);

        String complexJson =
                "{\n"
                        + "  \"type\": \"corporation\",\n"
                        + "  \"country\": \"USA\",\n"
                        + "  \"company\": {\n"
                        + "    \"name\": \"TechCorp\",\n"
                        + "    \"founded\": 2010,\n"
                        + "    \"departments\": [\n"
                        + "      {\"name\": \"Engineering\", \"budget\": 1000000},\n"
                        + "      {\"name\": \"Marketing\", \"budget\": 500000},\n"
                        + "      {\"name\": \"Sales\", \"budget\": 750000}\n"
                        + "    ]\n"
                        + "  }\n"
                        + "}";

        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {1L, "test", complexJson});
        inputRow.setTableId(catalogTable.getTableId().toTablePath().getFullName());

        List<SeaTunnelRow> result = transform.flatMap(inputRow);

        // Verify result: should have 3 rows (3 departments)
        Assertions.assertEquals(3, result.size());

        // Verify first row (Engineering department)
        SeaTunnelRow row1 = result.get(0);
        Assertions.assertEquals(1L, row1.getField(0)); // id
        Assertions.assertEquals("test", row1.getField(1)); // name
        Assertions.assertEquals(complexJson, row1.getField(2)); // data
        Assertions.assertEquals("Engineering", row1.getField(3)); // dept_name
        Assertions.assertEquals(1000000, row1.getField(4)); // dept_budget
        Assertions.assertEquals("TechCorp", row1.getField(5)); // company_name
        Assertions.assertEquals(2010, row1.getField(6)); // company_founded
        Assertions.assertEquals("corporation", row1.getField(7)); // organization_type
        Assertions.assertEquals("USA", row1.getField(8)); // organization_country

        // Verify second row (Marketing department)
        SeaTunnelRow row2 = result.get(1);
        Assertions.assertEquals("Marketing", row2.getField(3)); // dept_name
        Assertions.assertEquals(500000, row2.getField(4)); // dept_budget
        Assertions.assertEquals(
                "TechCorp",
                row2.getField(5)); // company_name (parent node field repeated in each row)

        // Verify third row (Sales department)
        SeaTunnelRow row3 = result.get(2);
        Assertions.assertEquals("Sales", row3.getField(3)); // dept_name
        Assertions.assertEquals(750000, row3.getField(4)); // dept_budget
        Assertions.assertEquals(
                "corporation",
                row3.getField(
                        7)); // organization_type (grandparent node field repeated in each row)
    }

    @Test
    public void testComplexNestedJsonNonArray() {
        // Test complex nested JSON non-array, arbitrary level parsing
        Map<String, Object> configMap = new HashMap<>();

        configMap.put("array_flatten_fields", new ArrayList<>());

        List<Map<String, Object>> outputFields =
                Arrays.asList(
                        // Level 1
                        createSingleFieldOutput("root_id", "string", "data", "$.id"),
                        createSingleFieldOutput("root_status", "string", "data", "$.status"),
                        // Level 2
                        createSingleFieldOutput("user_name", "string", "data", "$.user.name"),
                        createSingleFieldOutput("user_age", "int", "data", "$.user.age"),
                        // Level 3
                        createSingleFieldOutput(
                                "address_city", "string", "data", "$.user.profile.address.city"),
                        createSingleFieldOutput(
                                "address_zipcode",
                                "string",
                                "data",
                                "$.user.profile.address.zipcode"),
                        // Level 4
                        createSingleFieldOutput(
                                "contact_email",
                                "string",
                                "data",
                                "$.user.profile.contact.primary.email"),
                        createSingleFieldOutput(
                                "contact_phone",
                                "string",
                                "data",
                                "$.user.profile.contact.primary.phone"),
                        // Level 5 (deepest level)
                        createSingleFieldOutput(
                                "emergency_name",
                                "string",
                                "data",
                                "$.user.profile.contact.emergency.person.name"),
                        createSingleFieldOutput(
                                "emergency_relation",
                                "string",
                                "data",
                                "$.user.profile.contact.emergency.person.relation"),
                        // Single elements in array (non-expanded)
                        createSingleFieldOutput(
                                "first_hobby", "string", "data", "$.user.hobbies[0]"),
                        createSingleFieldOutput(
                                "second_hobby", "string", "data", "$.user.hobbies[1]"),
                        // Array elements in nested objects
                        createSingleFieldOutput(
                                "first_skill", "string", "data", "$.user.profile.skills[0].name"),
                        createSingleFieldOutput(
                                "skill_level", "string", "data", "$.user.profile.skills[0].level"));
        configMap.put("output_fields", outputFields);

        configMap.put("row_error_handle_way", "FAIL");
        configMap.put("field_merge_strategy", "APPEND_NEW");

        JsonPathFlatMapTransform transform = createTransform(configMap);

        String deepNestedJson =
                "{\n"
                        + "  \"id\": \"user_12345\",\n"
                        + "  \"status\": \"active\",\n"
                        + "  \"user\": {\n"
                        + "    \"name\": \"John Doe\",\n"
                        + "    \"age\": 30,\n"
                        + "    \"hobbies\": [\"reading\", \"swimming\", \"coding\"],\n"
                        + "    \"profile\": {\n"
                        + "      \"address\": {\n"
                        + "        \"city\": \"New York\",\n"
                        + "        \"zipcode\": \"10001\",\n"
                        + "        \"coordinates\": {\n"
                        + "          \"lat\": 40.7128,\n"
                        + "          \"lng\": -74.0060\n"
                        + "        }\n"
                        + "      },\n"
                        + "      \"contact\": {\n"
                        + "        \"primary\": {\n"
                        + "          \"email\": \"john.doe@example.com\",\n"
                        + "          \"phone\": \"+1-555-0123\"\n"
                        + "        },\n"
                        + "        \"emergency\": {\n"
                        + "          \"person\": {\n"
                        + "            \"name\": \"Jane Doe\",\n"
                        + "            \"relation\": \"spouse\",\n"
                        + "            \"contact\": {\n"
                        + "              \"phone\": \"+1-555-0456\"\n"
                        + "            }\n"
                        + "          }\n"
                        + "        }\n"
                        + "      },\n"
                        + "      \"skills\": [\n"
                        + "        {\"name\": \"Java\", \"level\": \"expert\"},\n"
                        + "        {\"name\": \"Python\", \"level\": \"intermediate\"}\n"
                        + "      ]\n"
                        + "    }\n"
                        + "  }\n"
                        + "}";

        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {1L, "test", deepNestedJson});
        inputRow.setTableId(catalogTable.getTableId().toTablePath().getFullName());

        List<SeaTunnelRow> result = transform.flatMap(inputRow);

        // Verify result: should have 1 row (non-array expansion)
        Assertions.assertEquals(1, result.size());
        SeaTunnelRow row = result.get(0);
        Assertions.assertEquals(1L, row.getField(0)); // id
        Assertions.assertEquals("test", row.getField(1)); // name
        Assertions.assertEquals(deepNestedJson, row.getField(2)); // data

        // Verify field extraction at different levels
        Assertions.assertEquals("user_12345", row.getField(3)); // root_id (Level 1)
        Assertions.assertEquals("active", row.getField(4)); // root_status (Level 1)
        Assertions.assertEquals("John Doe", row.getField(5)); // user_name (Level 2)
        Assertions.assertEquals(30, row.getField(6)); // user_age (Level 2)
        Assertions.assertEquals("New York", row.getField(7)); // address_city (Level 3)
        Assertions.assertEquals("10001", row.getField(8)); // address_zipcode (Level 3)
        Assertions.assertEquals("john.doe@example.com", row.getField(9)); // contact_email (Level 4)
        Assertions.assertEquals("+1-555-0123", row.getField(10)); // contact_phone (Level 4)
        Assertions.assertEquals("Jane Doe", row.getField(11)); // emergency_name (Level 5)
        Assertions.assertEquals("spouse", row.getField(12)); // emergency_relation (Level 5)
        Assertions.assertEquals("reading", row.getField(13)); // first_hobby (array element)
        Assertions.assertEquals("swimming", row.getField(14)); // second_hobby (array element)
        Assertions.assertEquals("Java", row.getField(15)); // first_skill (nested array element)
        Assertions.assertEquals("expert", row.getField(16)); // skill_level (nested array element)
    }

    @Test
    public void testTimeTypeErrorHandling() {
        // Test time type exception handling
        Map<String, Object> configMap = new HashMap<>();

        configMap.put("array_flatten_fields", new ArrayList<>());

        List<Map<String, Object>> outputFields =
                Arrays.asList(
                        createSingleFieldOutput(
                                "valid_timestamp", "timestamp", "data", "$.validTime"),
                        createSingleFieldOutput("valid_date", "date", "data", "$.validDate"),
                        createSingleFieldOutput("valid_time", "time", "data", "$.validTimeOnly"),
                        createSingleFieldOutputWithErrorHandling(
                                "invalid_timestamp", "timestamp", "data", "$.invalidTime", "SKIP"),
                        createSingleFieldOutputWithErrorHandling(
                                "invalid_date", "date", "data", "$.invalidDate", "SKIP"),
                        createSingleFieldOutputWithErrorHandling(
                                "invalid_time", "time", "data", "$.invalidTimeOnly", "SKIP"),
                        createSingleFieldOutputWithErrorHandling(
                                "missing_timestamp", "timestamp", "data", "$.missingTime", "SKIP"));
        configMap.put("output_fields", outputFields);

        configMap.put("row_error_handle_way", "FAIL");
        configMap.put("field_merge_strategy", "APPEND_NEW");

        JsonPathFlatMapTransform transform = createTransform(configMap);

        String timeJson =
                "{\n"
                        + "  \"validTime\": \"2023-12-25T10:30:00\",\n"
                        + "  \"validDate\": \"2023-12-25\",\n"
                        + "  \"validTimeOnly\": \"10:30:00\",\n"
                        + "  \"invalidTime\": \"not-a-timestamp\",\n"
                        + "  \"invalidDate\": \"invalid-date-format\",\n"
                        + "  \"invalidTimeOnly\": \"25:99:99\"\n"
                        + "}";

        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {1L, "test", timeJson});
        inputRow.setTableId(catalogTable.getTableId().toTablePath().getFullName());

        List<SeaTunnelRow> result = transform.flatMap(inputRow);

        Assertions.assertEquals(1, result.size());

        SeaTunnelRow row = result.get(0);
        Assertions.assertEquals(1L, row.getField(0)); // id
        Assertions.assertEquals("test", row.getField(1)); // name
        Assertions.assertEquals(timeJson, row.getField(2)); // data

        // Verify normal time fields (should be successfully converted)
        Assertions.assertNotNull(row.getField(3)); // valid_timestamp
        Assertions.assertNotNull(row.getField(4)); // valid_date
        Assertions.assertNotNull(row.getField(5)); // valid_time

        // Verify error time fields (should be skipped, value is null)
        Assertions.assertNull(row.getField(6)); // invalid_timestamp (SKIP)
        Assertions.assertNull(row.getField(7)); // invalid_date (SKIP)
        Assertions.assertNull(row.getField(8)); // invalid_time (SKIP)
        Assertions.assertNull(row.getField(9)); // missing_timestamp (SKIP)
    }

    @Test
    public void testTimeTypeErrorHandlingFail() {
        // Test time type exception handling - FAIL strategy
        Map<String, Object> configMap = new HashMap<>();

        configMap.put("array_flatten_fields", new ArrayList<>());

        List<Map<String, Object>> outputFields =
                Arrays.asList(
                        createSingleFieldOutput(
                                "valid_timestamp", "timestamp", "data", "$.validTime"),
                        createSingleFieldOutputWithErrorHandling(
                                "invalid_timestamp", "timestamp", "data", "$.invalidTime", "FAIL"));
        configMap.put("output_fields", outputFields);

        configMap.put("row_error_handle_way", "FAIL");
        configMap.put("field_merge_strategy", "APPEND_NEW");

        JsonPathFlatMapTransform transform = createTransform(configMap);

        String invalidTimeJson =
                "{\n"
                        + "  \"validTime\": \"2023-12-25T10:30:00\",\n"
                        + "  \"invalidTime\": \"definitely-not-a-timestamp\"\n"
                        + "}";

        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {1L, "test", invalidTimeJson});
        inputRow.setTableId(catalogTable.getTableId().toTablePath().getFullName());

        Assertions.assertThrows(
                ErrorDataTransformException.class,
                () -> {
                    transform.flatMap(inputRow);
                });
    }

    @Test
    public void testTimeTypeErrorHandlingSkipRow() {
        // Test time type exception handling - SKIP_ROW strategy
        Map<String, Object> configMap = new HashMap<>();

        configMap.put("array_flatten_fields", new ArrayList<>());

        List<Map<String, Object>> outputFields =
                Arrays.asList(
                        createSingleFieldOutput(
                                "valid_timestamp", "timestamp", "data", "$.validTime"),
                        createSingleFieldOutputWithErrorHandling(
                                "invalid_timestamp",
                                "timestamp",
                                "data",
                                "$.invalidTime",
                                "SKIP_ROW"));
        configMap.put("output_fields", outputFields);

        configMap.put("row_error_handle_way", "FAIL");
        configMap.put("field_merge_strategy", "APPEND_NEW");

        JsonPathFlatMapTransform transform = createTransform(configMap);

        // Create JSON containing invalid time data
        String invalidTimeJson =
                "{\n"
                        + "  \"validTime\": \"2023-12-25T10:30:00\",\n"
                        + "  \"invalidTime\": \"definitely-not-a-timestamp\"\n"
                        + "}";

        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {1L, "test", invalidTimeJson});
        inputRow.setTableId(catalogTable.getTableId().toTablePath().getFullName());

        List<SeaTunnelRow> result = transform.flatMap(inputRow);

        // Verify result: should skip entire row, return empty list
        Assertions.assertEquals(0, result.size());
    }

    private JsonPathFlatMapConfig createBasicConfig() {
        Map<String, Object> configMap = new HashMap<>();

        List<Map<String, Object>> arrayFlattenFields = new ArrayList<>();
        Map<String, Object> arrayField = new HashMap<>();
        arrayField.put("src_field", "data");
        arrayField.put("array_path", "$.items[*]");
        arrayField.put("field_key", "items");
        arrayFlattenFields.add(arrayField);
        configMap.put("array_flatten_fields", arrayFlattenFields);

        List<Map<String, Object>> outputFields = new ArrayList<>();
        Map<String, Object> outputField = new HashMap<>();
        outputField.put("dest_field", "item_name");
        outputField.put("dest_type", "string");
        outputField.put("from_array", "items");
        outputField.put("item_path", "$.name");
        outputFields.add(outputField);
        configMap.put("output_fields", outputFields);

        configMap.put("field_merge_strategy", "APPEND_NEW");
        configMap.put("flatten_strategy", "MAX_LENGTH_ALIGN");
        configMap.put("fill_strategy", "NULL");

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        return JsonPathFlatMapConfig.of(config, catalogTable);
    }

    private JsonPathFlatMapConfig createMaxLengthAlignConfig() {
        return createMultiArrayConfig("MAX_LENGTH_ALIGN");
    }

    private JsonPathFlatMapConfig createParallelExpandConfig() {
        return createMultiArrayConfig("PARALLEL_EXPAND");
    }

    private JsonPathFlatMapConfig createMultiArrayConfig(String strategy) {
        Map<String, Object> configMap = new HashMap<>();

        // Multiple array flatten field configurations
        List<Map<String, Object>> arrayFlattenFields = new ArrayList<>();

        Map<String, Object> arrayField1 = new HashMap<>();
        arrayField1.put("src_field", "data");
        arrayField1.put("array_path", "$.products[*]");
        arrayField1.put("field_key", "products");
        arrayFlattenFields.add(arrayField1);

        Map<String, Object> arrayField2 = new HashMap<>();
        arrayField2.put("src_field", "data");
        arrayField2.put("array_path", "$.categories[*]");
        arrayField2.put("field_key", "categories");
        arrayFlattenFields.add(arrayField2);

        configMap.put("array_flatten_fields", arrayFlattenFields);

        // Output field configurations
        List<Map<String, Object>> outputFields = new ArrayList<>();

        Map<String, Object> outputField1 = new HashMap<>();
        outputField1.put("dest_field", "product_name");
        outputField1.put("dest_type", "string");
        outputField1.put("from_array", "products");
        outputField1.put("item_path", "$.name");
        outputFields.add(outputField1);

        Map<String, Object> outputField2 = new HashMap<>();
        outputField2.put("dest_field", "category_name");
        outputField2.put("dest_type", "string");
        outputField2.put("from_array", "categories");
        outputField2.put("item_path", "$.name");
        outputFields.add(outputField2);

        configMap.put("output_fields", outputFields);
        configMap.put("field_merge_strategy", "APPEND_NEW");
        configMap.put("flatten_strategy", strategy);
        configMap.put("fill_strategy", "NULL");

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        return JsonPathFlatMapConfig.of(config, catalogTable);
    }

    private JsonPathFlatMapConfig createCartesianProductConfig() {
        Map<String, Object> configMap = new HashMap<>();

        // Multiple array flatten field configurations
        List<Map<String, Object>> arrayFlattenFields = new ArrayList<>();

        Map<String, Object> arrayField1 = new HashMap<>();
        arrayField1.put("src_field", "data");
        arrayField1.put("array_path", "$.products[*]");
        arrayField1.put("field_key", "products");
        arrayFlattenFields.add(arrayField1);

        Map<String, Object> arrayField2 = new HashMap<>();
        arrayField2.put("src_field", "data");
        arrayField2.put("array_path", "$.categories[*]");
        arrayField2.put("field_key", "categories");
        arrayFlattenFields.add(arrayField2);

        configMap.put("array_flatten_fields", arrayFlattenFields);

        // Output field configurations
        List<Map<String, Object>> outputFields = new ArrayList<>();

        Map<String, Object> outputField1 = new HashMap<>();
        outputField1.put("dest_field", "product_name");
        outputField1.put("dest_type", "string");
        outputField1.put("from_array", "products");
        outputField1.put("item_path", "$.name");
        outputFields.add(outputField1);

        Map<String, Object> outputField2 = new HashMap<>();
        outputField2.put("dest_field", "category_name");
        outputField2.put("dest_type", "string");
        outputField2.put("from_array", "categories");
        outputField2.put("item_path", "$.name");
        outputFields.add(outputField2);

        configMap.put("output_fields", outputFields);
        configMap.put("field_merge_strategy", "APPEND_NEW");
        configMap.put("flatten_strategy", "CARTESIAN_PRODUCT");
        configMap.put("fill_strategy", "NULL");

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        return JsonPathFlatMapConfig.of(config, catalogTable);
    }

    private Map<String, Object> createArrayFlattenFieldWithErrorHandling(
            String srcField, String arrayPath, String fieldKey, String errorHandleWay) {
        Map<String, Object> field = new HashMap<>();
        field.put("src_field", srcField);
        field.put("array_path", arrayPath);
        field.put("field_key", fieldKey);
        field.put("column_error_handle_way", errorHandleWay);
        return field;
    }

    private Map<String, Object> createSingleFieldOutput(
            String destField, String destType, String srcField, String itemPath) {
        Map<String, Object> field = new HashMap<>();
        field.put("dest_field", destField);
        field.put("dest_type", destType);
        field.put("src_field", srcField); // Use src_field for single field extraction
        field.put("item_path", itemPath);
        return field;
    }

    private Map<String, Object> createSingleFieldOutputWithErrorHandling(
            String destField,
            String destType,
            String srcField,
            String itemPath,
            String errorHandleWay) {
        Map<String, Object> field = new HashMap<>();
        field.put("dest_field", destField);
        field.put("dest_type", destType);
        field.put("src_field", srcField);
        field.put("item_path", itemPath);
        field.put("column_error_handle_way", errorHandleWay);
        return field;
    }

    @Test
    public void testAllDataTypesNormalCases() {
        // Test all supported data types with normal valid data
        Map<String, Object> configMap = new HashMap<>();

        configMap.put("array_flatten_fields", new ArrayList<>());

        List<Map<String, Object>> outputFields = Arrays.asList(
            // Basic types - normal cases
            createSingleFieldOutput("test_string", "string", "data", "$.string_field"),
            createSingleFieldOutput("test_boolean", "boolean", "data", "$.boolean_field"),
            createSingleFieldOutput("test_byte", "tinyint", "data", "$.byte_field"),
            createSingleFieldOutput("test_short", "smallint", "data", "$.short_field"),
            createSingleFieldOutput("test_int", "int", "data", "$.int_field"),
            createSingleFieldOutput("test_long", "bigint", "data", "$.long_field"),
            createSingleFieldOutput("test_float", "float", "data", "$.float_field"),
            createSingleFieldOutput("test_double", "double", "data", "$.double_field"),
            createSingleFieldOutput("test_decimal", "decimal(10,2)", "data", "$.decimal_field"),
            createSingleFieldOutput("test_date", "date", "data", "$.date_field"),
            createSingleFieldOutput("test_time", "time", "data", "$.time_field"),
            createSingleFieldOutput("test_timestamp", "timestamp", "data", "$.timestamp_field"),
            createSingleFieldOutput("test_bytes", "bytes", "data", "$.bytes_field")
        );
        configMap.put("output_fields", outputFields);

        configMap.put("row_error_handle_way", "FAIL");
        configMap.put("field_merge_strategy", "APPEND_NEW");
        configMap.put("flatten_strategy", "MAX_LENGTH_ALIGN");
        configMap.put("fill_strategy", "NULL");

        JsonPathFlatMapTransform transform = createTransform(configMap);

        // Create test data with all valid types
        String allTypesJson = "{\n" +
            "  \"string_field\": \"test_string_value\",\n" +
            "  \"boolean_field\": true,\n" +
            "  \"byte_field\": 127,\n" +
            "  \"short_field\": 32767,\n" +
            "  \"int_field\": 2147483647,\n" +
            "  \"long_field\": 9223372036854775807,\n" +
            "  \"float_field\": 3.14159,\n" +
            "  \"double_field\": 2.718281828459045,\n" +
            "  \"decimal_field\": \"123.45\",\n" +
            "  \"date_field\": \"2023-12-25\",\n" +
            "  \"time_field\": \"10:30:45\",\n" +
            "  \"timestamp_field\": \"2023-12-25T10:30:45\",\n" +
            "  \"bytes_field\": \"SGVsbG8gV29ybGQ=\"\n" +
            "}";

        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[]{
            1L, "test", allTypesJson
        });
        inputRow.setTableId(catalogTable.getTableId().toTablePath().getFullName());

        List<SeaTunnelRow> result = transform.flatMap(inputRow);

        // Verify result: should have 1 row with all types converted successfully
        Assertions.assertEquals(1, result.size());
        SeaTunnelRow row = result.get(0);

        // Verify source fields
        Assertions.assertEquals(1L, row.getField(0));  // id
        Assertions.assertEquals("test", row.getField(1));  // name
        Assertions.assertEquals(allTypesJson, row.getField(2));  // data

        // Verify converted fields
        Assertions.assertEquals("test_string_value", row.getField(3));  // test_string
        Assertions.assertEquals(true, row.getField(4));  // test_boolean
        Assertions.assertEquals((byte) 127, row.getField(5));  // test_byte
        Assertions.assertEquals((short) 32767, row.getField(6));  // test_short
        Assertions.assertEquals(2147483647, row.getField(7));  // test_int
        Assertions.assertEquals(9223372036854775807L, row.getField(8));  // test_long
        Assertions.assertNotNull(row.getField(9));  // test_float
        Assertions.assertNotNull(row.getField(10));  // test_double
        Assertions.assertNotNull(row.getField(11));  // test_decimal
        Assertions.assertNotNull(row.getField(12));  // test_date
        Assertions.assertNotNull(row.getField(13));  // test_time
        Assertions.assertNotNull(row.getField(14));  // test_timestamp
        Assertions.assertNotNull(row.getField(15));  // test_bytes
    }

    @Test
    public void testAllDataTypesErrorCases() {
        // Test all supported data types with invalid data to trigger conversion errors
        Map<String, Object> configMap = new HashMap<>();

        configMap.put("array_flatten_fields", new ArrayList<>());

        List<Map<String, Object>> outputFields = Arrays.asList(
            // Basic types - error cases with SKIP error handling
            createSingleFieldOutputWithErrorHandling("test_string", "string", "data", "$.missing_string", "SKIP"),
            createSingleFieldOutputWithErrorHandling("test_boolean", "boolean", "data", "$.invalid_boolean", "SKIP"),
            createSingleFieldOutputWithErrorHandling("test_byte", "tinyint", "data", "$.invalid_byte", "SKIP"),
            createSingleFieldOutputWithErrorHandling("test_short", "smallint", "data", "$.invalid_short", "SKIP"),
            createSingleFieldOutputWithErrorHandling("test_int", "int", "data", "$.invalid_int", "SKIP"),
            createSingleFieldOutputWithErrorHandling("test_long", "bigint", "data", "$.invalid_long", "SKIP"),
            createSingleFieldOutputWithErrorHandling("test_float", "float", "data", "$.invalid_float", "SKIP"),
            createSingleFieldOutputWithErrorHandling("test_double", "double", "data", "$.invalid_double", "SKIP"),
            createSingleFieldOutputWithErrorHandling("test_decimal", "decimal(10,2)", "data", "$.invalid_decimal", "SKIP"),
            createSingleFieldOutputWithErrorHandling("test_date", "date", "data", "$.invalid_date", "SKIP"),
            createSingleFieldOutputWithErrorHandling("test_time", "time", "data", "$.invalid_time", "SKIP"),
            createSingleFieldOutputWithErrorHandling("test_timestamp", "timestamp", "data", "$.invalid_timestamp", "SKIP"),
            createSingleFieldOutputWithErrorHandling("test_bytes", "bytes", "data", "$.invalid_bytes", "SKIP")
        );
        configMap.put("output_fields", outputFields);

        configMap.put("row_error_handle_way", "FAIL");
        configMap.put("field_merge_strategy", "APPEND_NEW");
        configMap.put("flatten_strategy", "MAX_LENGTH_ALIGN");
        configMap.put("fill_strategy", "NULL");

        JsonPathFlatMapTransform transform = createTransform(configMap);

        // Create test data with invalid values for type conversion
        String invalidTypesJson = "{\n" +
            "  \"invalid_boolean\": \"not_a_boolean\",\n" +
            "  \"invalid_byte\": \"not_a_byte\",\n" +
            "  \"invalid_short\": \"not_a_short\",\n" +
            "  \"invalid_int\": \"not_an_int\",\n" +
            "  \"invalid_long\": \"not_a_long\",\n" +
            "  \"invalid_float\": \"not_a_float\",\n" +
            "  \"invalid_double\": \"not_a_double\",\n" +
            "  \"invalid_decimal\": \"not_a_decimal\",\n" +
            "  \"invalid_date\": \"not_a_date\",\n" +
            "  \"invalid_time\": \"not_a_time\",\n" +
            "  \"invalid_timestamp\": \"not_a_timestamp\",\n" +
            "  \"invalid_bytes\": \"invalid_base64!!!\"\n" +
            "}";

        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[]{
            1L, "test", invalidTypesJson
        });
        inputRow.setTableId(catalogTable.getTableId().toTablePath().getFullName());

        List<SeaTunnelRow> result = transform.flatMap(inputRow);

        // Verify result: should have 1 row with all error fields skipped (filled with null)
        Assertions.assertEquals(1, result.size());
        SeaTunnelRow row = result.get(0);

        // Verify source fields
        Assertions.assertEquals(1L, row.getField(0));  // id
        Assertions.assertEquals("test", row.getField(1));  // name
        Assertions.assertEquals(invalidTypesJson, row.getField(2));  // data
        Assertions.assertNull(row.getField(3));  // test_string
        Assertions.assertEquals(false, row.getField(4));  // test_boolean

        // Verify all error fields are skipped (filled with null)
        for (int i = 5; i < 16; i++) {
            Assertions.assertNull(row.getField(i), "Field at index " + i + " should be null due to conversion error");
        }
    }
}
