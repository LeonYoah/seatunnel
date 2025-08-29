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
import org.apache.seatunnel.transform.jsonpathflatmap.JsonPathFlatMapConfig.ArrayFlattenFieldConfig;
import org.apache.seatunnel.transform.jsonpathflatmap.JsonPathFlatMapConfig.ArrayFlattenStrategy;
import org.apache.seatunnel.transform.jsonpathflatmap.JsonPathFlatMapConfig.FieldMergeStrategy;
import org.apache.seatunnel.transform.jsonpathflatmap.JsonPathFlatMapConfig.FillStrategy;
import org.apache.seatunnel.transform.jsonpathflatmap.JsonPathFlatMapConfig.OutputFieldConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JsonPathFlatMapConfigTest {

    private CatalogTable catalogTable;

    @BeforeEach
    public void setUp() {
        // Create test CatalogTable
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
    }

    @Test
    public void testBasicConfiguration() {
        // Prepare configuration data
        Map<String, Object> configMap = new HashMap<>();

        // Array flatten field configuration
        List<Map<String, Object>> arrayFlattenFields = new ArrayList<>();
        Map<String, Object> arrayField = new HashMap<>();
        arrayField.put("src_field", "data");
        arrayField.put("array_path", "$.items[*]");
        arrayField.put("field_key", "items");
        arrayFlattenFields.add(arrayField);
        configMap.put("array_flatten_fields", arrayFlattenFields);

        // Output field configuration
        List<Map<String, Object>> outputFields = new ArrayList<>();
        Map<String, Object> outputField = new HashMap<>();
        outputField.put("dest_field", "item_name");
        outputField.put("dest_type", "string");
        outputField.put("from_array", "items");
        outputField.put("item_path", "$.name");
        outputFields.add(outputField);
        configMap.put("output_fields", outputFields);

        // Other configurations
        configMap.put("field_merge_strategy", "APPEND_NEW");
        configMap.put("flatten_strategy", "MAX_LENGTH_ALIGN");
        configMap.put("fill_strategy", "NULL");

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        // Create configuration object
        JsonPathFlatMapConfig flatMapConfig = JsonPathFlatMapConfig.of(config, catalogTable);

        // Verify configuration
        Assertions.assertNotNull(flatMapConfig);
        Assertions.assertEquals(1, flatMapConfig.getArrayFlattenFields().size());
        Assertions.assertEquals(1, flatMapConfig.getOutputFields().size());
        Assertions.assertEquals(
                FieldMergeStrategy.APPEND_NEW, flatMapConfig.getFieldMergeStrategy());
        Assertions.assertEquals(
                ArrayFlattenStrategy.MAX_LENGTH_ALIGN, flatMapConfig.getFlattenStrategy());
        Assertions.assertEquals(FillStrategy.NULL, flatMapConfig.getFillStrategy());

        // Verify array flatten field configuration
        ArrayFlattenFieldConfig arrayFlattenFieldConfig =
                flatMapConfig.getArrayFlattenFields().get(0);
        Assertions.assertEquals("data", arrayFlattenFieldConfig.getSrcField());
        Assertions.assertEquals("$.items[*]", arrayFlattenFieldConfig.getArrayPath());
        Assertions.assertEquals("items", arrayFlattenFieldConfig.getFieldKey());
        Assertions.assertFalse(arrayFlattenFieldConfig.isRootArray());

        // Verify output field configuration
        OutputFieldConfig outputFieldConfig = flatMapConfig.getOutputFields().get(0);
        Assertions.assertEquals("item_name", outputFieldConfig.getDestField());
        Assertions.assertEquals("items", outputFieldConfig.getFromArray());
        Assertions.assertEquals("$.name", outputFieldConfig.getItemPath());
    }

    @Test
    public void testRootArrayConfiguration() {
        // Test root array configuration
        Map<String, Object> configMap = new HashMap<>();

        // Root array flatten field configuration
        List<Map<String, Object>> arrayFlattenFields = new ArrayList<>();
        Map<String, Object> arrayField = new HashMap<>();
        arrayField.put("src_field", "data");
        arrayField.put("array_path", "$"); // Root array path
        arrayField.put("field_key", "root_items");
        arrayFlattenFields.add(arrayField);
        configMap.put("array_flatten_fields", arrayFlattenFields);

        // Output field configuration
        List<Map<String, Object>> outputFields = new ArrayList<>();
        Map<String, Object> outputField = new HashMap<>();
        outputField.put("dest_field", "item_id");
        outputField.put("dest_type", "long");
        outputField.put("from_array", "root_items");
        outputField.put("item_path", "$.id");
        outputFields.add(outputField);
        configMap.put("output_fields", outputFields);

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        JsonPathFlatMapConfig flatMapConfig = JsonPathFlatMapConfig.of(config, catalogTable);

        // Verify root array configuration
        ArrayFlattenFieldConfig arrayFlattenFieldConfig =
                flatMapConfig.getArrayFlattenFields().get(0);
        Assertions.assertTrue(arrayFlattenFieldConfig.isRootArray());
        Assertions.assertEquals("$[*]", arrayFlattenFieldConfig.getActualArrayPath());
    }

    @Test
    public void testMultipleArraysConfiguration() {
        // Test multiple arrays configuration
        Map<String, Object> configMap = new HashMap<>();

        // Multiple array flatten field configuration
        List<Map<String, Object>> arrayFlattenFields = new ArrayList<>();

        // First array
        Map<String, Object> arrayField1 = new HashMap<>();
        arrayField1.put("src_field", "data");
        arrayField1.put("array_path", "$.products[*]");
        arrayField1.put("field_key", "products");
        arrayFlattenFields.add(arrayField1);

        // Second array
        Map<String, Object> arrayField2 = new HashMap<>();
        arrayField2.put("src_field", "data");
        arrayField2.put("array_path", "$.categories[*]");
        arrayField2.put("field_key", "categories");
        arrayFlattenFields.add(arrayField2);

        configMap.put("array_flatten_fields", arrayFlattenFields);

        // Multiple output field configuration
        List<Map<String, Object>> outputFields = new ArrayList<>();

        // Product field
        Map<String, Object> outputField1 = new HashMap<>();
        outputField1.put("dest_field", "product_name");
        outputField1.put("dest_type", "string");
        outputField1.put("from_array", "products");
        outputField1.put("item_path", "$.name");
        outputFields.add(outputField1);

        // Category field
        Map<String, Object> outputField2 = new HashMap<>();
        outputField2.put("dest_field", "category_name");
        outputField2.put("dest_type", "string");
        outputField2.put("from_array", "categories");
        outputField2.put("item_path", "$.name");
        outputFields.add(outputField2);

        configMap.put("output_fields", outputFields);
        configMap.put("flatten_strategy", "CARTESIAN_PRODUCT");

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        JsonPathFlatMapConfig flatMapConfig = JsonPathFlatMapConfig.of(config, catalogTable);

        // Verify multiple arrays configuration
        Assertions.assertEquals(2, flatMapConfig.getArrayFlattenFields().size());
        Assertions.assertEquals(2, flatMapConfig.getOutputFields().size());
        Assertions.assertEquals(
                ArrayFlattenStrategy.CARTESIAN_PRODUCT, flatMapConfig.getFlattenStrategy());
    }

    @Test
    public void testKeepSourceOrderStrategy() {
        // Test keep source order strategy
        Map<String, Object> configMap = new HashMap<>();

        // Array flatten field configuration
        List<Map<String, Object>> arrayFlattenFields = new ArrayList<>();
        Map<String, Object> arrayField = new HashMap<>();
        arrayField.put("src_field", "data");
        arrayField.put("array_path", "$.items[*]");
        arrayField.put("field_key", "items");
        arrayFlattenFields.add(arrayField);
        configMap.put("array_flatten_fields", arrayFlattenFields);

        // Output field configuration
        List<Map<String, Object>> outputFields = new ArrayList<>();
        Map<String, Object> outputField = new HashMap<>();
        outputField.put("dest_field", "item_value");
        outputField.put("dest_type", "string");
        outputField.put("from_array", "items");
        outputField.put("item_path", "$.value");
        outputFields.add(outputField);
        configMap.put("output_fields", outputFields);

        // Field merge strategy and preserve fields
        configMap.put("field_merge_strategy", "KEEP_SOURCE_ORDER");
        List<String> preserveFields = new ArrayList<>();
        preserveFields.add("id");
        preserveFields.add("name");
        configMap.put("preserve_source_fields", preserveFields);

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        JsonPathFlatMapConfig flatMapConfig = JsonPathFlatMapConfig.of(config, catalogTable);

        // Verify keep source order strategy
        Assertions.assertEquals(
                FieldMergeStrategy.KEEP_SOURCE_ORDER, flatMapConfig.getFieldMergeStrategy());
        Assertions.assertNotNull(flatMapConfig.getPreserveSourceFields());
        Assertions.assertEquals(2, flatMapConfig.getPreserveSourceFields().size());
        Assertions.assertTrue(flatMapConfig.getPreserveSourceFields().contains("id"));
        Assertions.assertTrue(flatMapConfig.getPreserveSourceFields().contains("name"));
    }

    @Test
    public void testInvalidConfiguration() {
        // Test invalid configuration
        Map<String, Object> configMap = new HashMap<>();

        // Missing required array_flatten_fields
        List<Map<String, Object>> outputFields = new ArrayList<>();
        Map<String, Object> outputField = new HashMap<>();
        outputField.put("dest_field", "item_name");
        outputField.put("dest_type", "string");
        outputField.put("from_array", "items");
        outputField.put("item_path", "$.name");
        outputFields.add(outputField);
        configMap.put("output_fields", outputFields);

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        // Should throw exception
        Assertions.assertThrows(
                Exception.class,
                () -> {
                    JsonPathFlatMapConfig.of(config, catalogTable);
                });
    }

    @Test
    public void testInvalidSourceField() {
        // Test invalid source field
        Map<String, Object> configMap = new HashMap<>();

        // Use non-existent source field
        List<Map<String, Object>> arrayFlattenFields = new ArrayList<>();
        Map<String, Object> arrayField = new HashMap<>();
        arrayField.put("src_field", "non_existent_field"); // Non-existent field
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

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        // Should throw exception
        Assertions.assertThrows(
                Exception.class,
                () -> {
                    JsonPathFlatMapConfig.of(config, catalogTable);
                });
    }
}
