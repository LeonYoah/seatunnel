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
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.format.json.JsonToRowConverters;
import org.apache.seatunnel.transform.common.ErrorHandleWay;
import org.apache.seatunnel.transform.exception.ErrorDataTransformException;
import org.apache.seatunnel.transform.jsonpathflatmap.JsonPathFlatMapConfig.FieldMergeStrategy;
import org.apache.seatunnel.transform.jsonpathflatmap.JsonPathFlatMapConfig.OutputFieldConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FieldMappingInfoTest {

    private SeaTunnelRowType inputRowType;
    private List<OutputFieldConfig> outputFields;
    private JsonToRowConverters.JsonToObjectConverter[] converters;

    @BeforeEach
    public void setUp() {
        // Create input row type
        String[] fieldNames = {"id", "name", "data"};
        org.apache.seatunnel.api.table.type.SeaTunnelDataType<?>[] fieldTypes = {
            BasicType.LONG_TYPE, BasicType.STRING_TYPE, BasicType.STRING_TYPE
        };
        this.inputRowType = new SeaTunnelRowType(fieldNames, fieldTypes);

        // Create output field configurations
        this.outputFields = new ArrayList<>();

        Column itemNameColumn =
                PhysicalColumn.of("item_name", BasicType.STRING_TYPE, 0, true, null, null);
        OutputFieldConfig itemNameConfig =
                new OutputFieldConfig(
                        "item_name", itemNameColumn, "items", null, "$.name", null, null);
        outputFields.add(itemNameConfig);

        Column itemValueColumn =
                PhysicalColumn.of("item_value", BasicType.INT_TYPE, 0, true, null, null);
        OutputFieldConfig itemValueConfig =
                new OutputFieldConfig(
                        "item_value", itemValueColumn, "items", null, "$.value", null, null);
        outputFields.add(itemValueConfig);

        // Create converters
        JsonToRowConverters jsonToRowConverters = new JsonToRowConverters(false, false);
        this.converters =
                outputFields.stream()
                        .map(OutputFieldConfig::getDestColumn)
                        .map(Column::getDataType)
                        .map(jsonToRowConverters::createConverter)
                        .toArray(JsonToRowConverters.JsonToObjectConverter[]::new);
    }

    @Test
    public void testAppendNewStrategy() {
        // Test append new fields strategy
        FieldMappingInfo mappingInfo =
                new FieldMappingInfo(
                        inputRowType, outputFields, FieldMergeStrategy.APPEND_NEW, null);

        // Create test data
        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {1L, "test", "{}"});
        Map<String, List<Object>> arrayDataMap = new HashMap<>();
        arrayDataMap.put(
                "items",
                Arrays.asList(createJsonObject("item1", 100), createJsonObject("item2", 200)));
        Map<String, Integer> arrayIndexMap = new HashMap<>();
        arrayIndexMap.put("items", 0);

        // Create ProcessedData object
        ProcessedData processedData = new ProcessedData(arrayDataMap, new HashMap<>());

        // Create output fields
        Object[] outputFields =
                mappingInfo.createOutputFields(
                        inputRow, processedData, arrayIndexMap, converters, null);

        // Verify result - should have 5 fields: 3 source fields + 2 output fields
        Assertions.assertEquals(5, outputFields.length);
        Assertions.assertEquals(1L, outputFields[0]); // id (source field)
        Assertions.assertEquals("test", outputFields[1]); // name (source field)
        Assertions.assertEquals("{}", outputFields[2]); // data (source field)
        Assertions.assertEquals("item1", outputFields[3]); // item_name (output field)
        Assertions.assertEquals(100, outputFields[4]); // item_value (output field)
    }

    @Test
    public void testReplaceAllStrategy() {
        // Test replace all strategy
        FieldMappingInfo mappingInfo =
                new FieldMappingInfo(
                        inputRowType, outputFields, FieldMergeStrategy.REPLACE_ALL, null);

        // Create test data
        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {1L, "test", "{}"});
        Map<String, List<Object>> arrayDataMap = new HashMap<>();
        arrayDataMap.put(
                "items",
                Arrays.asList(createJsonObject("item1", 100), createJsonObject("item2", 200)));
        Map<String, Integer> arrayIndexMap = new HashMap<>();
        arrayIndexMap.put("items", 0);

        // Create ProcessedData object
        ProcessedData processedData = new ProcessedData(arrayDataMap, new HashMap<>());

        // Create output fields
        Object[] outputFields =
                mappingInfo.createOutputFields(
                        inputRow, processedData, arrayIndexMap, converters, null);

        // Verify result - should have only 2 output fields
        Assertions.assertEquals(2, outputFields.length);
        Assertions.assertEquals("item1", outputFields[0]); // item_name
        Assertions.assertEquals(100, outputFields[1]); // item_value
    }

    @Test
    public void testKeepSourceOrderStrategy() {
        // Test keep source order strategy
        List<String> preserveSourceFields = Arrays.asList("id", "name");
        FieldMappingInfo mappingInfo =
                new FieldMappingInfo(
                        inputRowType,
                        outputFields,
                        FieldMergeStrategy.KEEP_SOURCE_ORDER,
                        preserveSourceFields);

        // Create test data
        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {1L, "test", "{}"});
        Map<String, List<Object>> arrayDataMap = new HashMap<>();
        arrayDataMap.put(
                "items",
                Arrays.asList(createJsonObject("item1", 100), createJsonObject("item2", 200)));
        Map<String, Integer> arrayIndexMap = new HashMap<>();
        arrayIndexMap.put("items", 0);

        // Create ProcessedData object
        ProcessedData processedData = new ProcessedData(arrayDataMap, new HashMap<>());

        // Create output fields
        Object[] outputFields =
                mappingInfo.createOutputFields(
                        inputRow, processedData, arrayIndexMap, converters, null);

        // Verify result - should have 4 fields: 2 preserved source fields + 2 output fields
        Assertions.assertEquals(4, outputFields.length);
        Assertions.assertEquals(1L, outputFields[0]); // id (preserved source field)
        Assertions.assertEquals("test", outputFields[1]); // name (preserved source field)
        Assertions.assertEquals("item1", outputFields[2]); // item_name (output field)
        Assertions.assertEquals(100, outputFields[3]); // item_value (output field)
    }

    @Test
    public void testKeepSourceOrderWithFieldReplacement() {
        // Test keep source order strategy with field replacement
        List<OutputFieldConfig> outputFieldsWithReplacement = new ArrayList<>();

        // Create an output field with the same name as source field
        Column nameColumn = PhysicalColumn.of("name", BasicType.STRING_TYPE, 0, true, null, null);
        OutputFieldConfig nameConfig =
                new OutputFieldConfig("name", nameColumn, "items", null, "$.name", null, null);
        outputFieldsWithReplacement.add(nameConfig);

        Column itemValueColumn =
                PhysicalColumn.of("item_value", BasicType.INT_TYPE, 0, true, null, null);
        OutputFieldConfig itemValueConfig =
                new OutputFieldConfig(
                        "item_value", itemValueColumn, "items", null, "$.value", null, null);
        outputFieldsWithReplacement.add(itemValueConfig);

        FieldMappingInfo mappingInfo =
                new FieldMappingInfo(
                        inputRowType,
                        outputFieldsWithReplacement,
                        FieldMergeStrategy.KEEP_SOURCE_ORDER,
                        null);

        // Create test data
        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {1L, "test", "{}"});
        Map<String, List<Object>> arrayDataMap = new HashMap<>();
        arrayDataMap.put(
                "items",
                Arrays.asList(createJsonObject("item1", 100), createJsonObject("item2", 200)));
        Map<String, Integer> arrayIndexMap = new HashMap<>();
        arrayIndexMap.put("items", 0);

        // Recreate converters
        JsonToRowConverters jsonToRowConverters = new JsonToRowConverters(false, false);
        JsonToRowConverters.JsonToObjectConverter[] newConverters =
                outputFieldsWithReplacement.stream()
                        .map(OutputFieldConfig::getDestColumn)
                        .map(Column::getDataType)
                        .map(jsonToRowConverters::createConverter)
                        .toArray(JsonToRowConverters.JsonToObjectConverter[]::new);

        // Create ProcessedData object
        ProcessedData processedData = new ProcessedData(arrayDataMap, new HashMap<>());

        // Create output fields
        Object[] outputFields =
                mappingInfo.createOutputFields(
                        inputRow, processedData, arrayIndexMap, newConverters, null);

        // Verify result - name field should be replaced
        Assertions.assertEquals(4, outputFields.length);
        Assertions.assertEquals(1L, outputFields[0]); // id (source field)
        Assertions.assertEquals("item1", outputFields[1]); // name (replaced with output field)
        Assertions.assertEquals("{}", outputFields[2]); // data (source field)
        Assertions.assertEquals(100, outputFields[3]); // item_value (new output field)
    }

    @Test
    public void testCreateOutputTableSchema() {
        // Test creating output table schema
        FieldMappingInfo mappingInfo =
                new FieldMappingInfo(
                        inputRowType, outputFields, FieldMergeStrategy.APPEND_NEW, null);

        // Create input table schema
        List<Column> inputColumns = new ArrayList<>();
        inputColumns.add(PhysicalColumn.of("id", BasicType.LONG_TYPE, 0, false, null, null));
        inputColumns.add(PhysicalColumn.of("name", BasicType.STRING_TYPE, 0, true, null, null));
        inputColumns.add(PhysicalColumn.of("data", BasicType.STRING_TYPE, 0, true, null, null));
        TableSchema inputTableSchema = TableSchema.builder().columns(inputColumns).build();

        // Create output table schema
        TableSchema outputTableSchema = mappingInfo.createOutputTableSchema(inputTableSchema);

        // Verify result
        Assertions.assertNotNull(outputTableSchema);
        Assertions.assertEquals(5, outputTableSchema.getColumns().size());

        // Verify field order and types
        List<Column> columns = outputTableSchema.getColumns();
        Assertions.assertEquals("id", columns.get(0).getName());
        Assertions.assertEquals(BasicType.LONG_TYPE, columns.get(0).getDataType());
        Assertions.assertEquals("name", columns.get(1).getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, columns.get(1).getDataType());
        Assertions.assertEquals("data", columns.get(2).getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, columns.get(2).getDataType());
        Assertions.assertEquals("item_name", columns.get(3).getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, columns.get(3).getDataType());
        Assertions.assertEquals("item_value", columns.get(4).getName());
        Assertions.assertEquals(BasicType.INT_TYPE, columns.get(4).getDataType());
    }

    @Test
    public void testMissingArrayData() {
        // Test handling missing array data
        FieldMappingInfo mappingInfo =
                new FieldMappingInfo(
                        inputRowType, outputFields, FieldMergeStrategy.APPEND_NEW, null);

        // Create test data - missing array data
        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {1L, "test", "{}"});
        Map<String, List<Object>> arrayDataMap = new HashMap<>(); // Empty array data
        Map<String, Integer> arrayIndexMap = new HashMap<>();

        // Create ProcessedData object
        ProcessedData processedData = new ProcessedData(arrayDataMap, new HashMap<>());

        // Create output fields
        Object[] outputFields =
                mappingInfo.createOutputFields(
                        inputRow, processedData, arrayIndexMap, converters, null);

        // Verify result - output fields should be null
        Assertions.assertEquals(5, outputFields.length);
        Assertions.assertEquals(1L, outputFields[0]); // id (source field)
        Assertions.assertEquals("test", outputFields[1]); // name (source field)
        Assertions.assertEquals("{}", outputFields[2]); // data (source field)
        Assertions.assertNull(outputFields[3]); // item_name (null)
        Assertions.assertNull(outputFields[4]); // item_value (null)
    }

    @Test
    public void testArrayIndexOutOfBounds() {
        // Test handling array index out of bounds when no fill strategy is set
        FieldMappingInfo mappingInfo =
                new FieldMappingInfo(
                        inputRowType, outputFields, FieldMergeStrategy.APPEND_NEW, null);

        // Create test data
        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {1L, "test", "{}"});
        Map<String, List<Object>> arrayDataMap = new HashMap<>();
        arrayDataMap.put("items", Arrays.asList(createJsonObject("item1", 100)));
        Map<String, Integer> arrayIndexMap = new HashMap<>();
        arrayIndexMap.put("items", 5); // Index out of bounds

        // Create ProcessedData object
        ProcessedData processedData = new ProcessedData(arrayDataMap, new HashMap<>());

        // Verify result - should throw exception prompting user to set fill strategy
        ErrorDataTransformException exception =
                Assertions.assertThrows(
                        ErrorDataTransformException.class,
                        () -> {
                            mappingInfo.createOutputFields(
                                    inputRow, processedData, arrayIndexMap, converters, null);
                        });

        // Verify exception message contains correct prompt information
        Assertions.assertTrue(exception.getMessage().contains("Array index out of bounds"));
        Assertions.assertTrue(exception.getMessage().contains("Please configure fill_strategy"));
        Assertions.assertTrue(exception.getMessage().contains("NULL, REPEAT_LAST, REPEAT_FIRST"));
    }

    @Test
    public void testErrorHandlingSkip() {
        // Test error handling - SKIP strategy
        List<OutputFieldConfig> outputFieldsWithError = new ArrayList<>();

        // Create an error-prone output field configuration (incorrect JsonPath)
        Column errorColumn =
                PhysicalColumn.of("error_field", BasicType.STRING_TYPE, 0, true, null, null);
        OutputFieldConfig errorConfig =
                new OutputFieldConfig(
                        "error_field",
                        errorColumn,
                        "items",
                        null,
                        "$.nonexistent.field",
                        null,
                        ErrorHandleWay.SKIP);
        outputFieldsWithError.add(errorConfig);

        // Create a normal output field
        Column normalColumn =
                PhysicalColumn.of("normal_field", BasicType.STRING_TYPE, 0, true, null, null);
        OutputFieldConfig normalConfig =
                new OutputFieldConfig(
                        "normal_field", normalColumn, "items", null, "$.name", null, null);
        outputFieldsWithError.add(normalConfig);

        FieldMappingInfo mappingInfo =
                new FieldMappingInfo(
                        inputRowType, outputFieldsWithError, FieldMergeStrategy.APPEND_NEW, null);

        // Create test data
        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {1L, "test", "{}"});
        Map<String, List<Object>> arrayDataMap = new HashMap<>();
        arrayDataMap.put("items", Arrays.asList(createJsonObject("item1", 100)));
        Map<String, Integer> arrayIndexMap = new HashMap<>();
        arrayIndexMap.put("items", 0);

        // Recreate converters
        JsonToRowConverters jsonToRowConverters = new JsonToRowConverters(false, false);
        JsonToRowConverters.JsonToObjectConverter[] newConverters =
                outputFieldsWithError.stream()
                        .map(OutputFieldConfig::getDestColumn)
                        .map(Column::getDataType)
                        .map(jsonToRowConverters::createConverter)
                        .toArray(JsonToRowConverters.JsonToObjectConverter[]::new);

        // Create ProcessedData object
        ProcessedData processedData = new ProcessedData(arrayDataMap, new HashMap<>());

        // Create output fields - error field should be skipped
        Object[] outputFields =
                mappingInfo.createOutputFields(
                        inputRow, processedData, arrayIndexMap, newConverters, ErrorHandleWay.FAIL);

        // Verify result - error field is skipped (null), normal field is processed normally
        Assertions.assertEquals(5, outputFields.length);
        Assertions.assertEquals(1L, outputFields[0]); // id (source field)
        Assertions.assertEquals("test", outputFields[1]); // name (source field)
        Assertions.assertEquals("{}", outputFields[2]); // data (source field)
        Assertions.assertNull(outputFields[3]); // error_field (skipped)
        Assertions.assertEquals("item1", outputFields[4]); // normal_field (processed normally)
    }

    @Test
    public void testErrorHandlingFail() {
        // Test error handling - FAIL strategy
        List<OutputFieldConfig> outputFieldsWithError = new ArrayList<>();

        // Create an error-prone output field configuration, set to FAIL
        Column errorColumn =
                PhysicalColumn.of("error_field", BasicType.STRING_TYPE, 0, true, null, null);
        OutputFieldConfig errorConfig =
                new OutputFieldConfig(
                        "error_field",
                        errorColumn,
                        "items",
                        null,
                        "$.nonexistent.field",
                        null,
                        ErrorHandleWay.FAIL);
        outputFieldsWithError.add(errorConfig);

        FieldMappingInfo mappingInfo =
                new FieldMappingInfo(
                        inputRowType, outputFieldsWithError, FieldMergeStrategy.APPEND_NEW, null);

        // Create test data
        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {1L, "test", "{}"});
        Map<String, List<Object>> arrayDataMap = new HashMap<>();
        arrayDataMap.put("items", Arrays.asList(createJsonObject("item1", 100)));
        Map<String, Integer> arrayIndexMap = new HashMap<>();
        arrayIndexMap.put("items", 0);

        // Recreate converters
        JsonToRowConverters jsonToRowConverters = new JsonToRowConverters(false, false);
        JsonToRowConverters.JsonToObjectConverter[] newConverters =
                outputFieldsWithError.stream()
                        .map(OutputFieldConfig::getDestColumn)
                        .map(Column::getDataType)
                        .map(jsonToRowConverters::createConverter)
                        .toArray(JsonToRowConverters.JsonToObjectConverter[]::new);

        // Create ProcessedData object
        ProcessedData processedData = new ProcessedData(arrayDataMap, new HashMap<>());

        // Create output fields - should throw exception
        Assertions.assertThrows(
                ErrorDataTransformException.class,
                () -> {
                    mappingInfo.createOutputFields(
                            inputRow,
                            processedData,
                            arrayIndexMap,
                            newConverters,
                            ErrorHandleWay.FAIL);
                });
    }

    @Test
    public void testErrorHandlingSkipRow() {
        // Test error handling - SKIP_ROW strategy
        List<OutputFieldConfig> outputFieldsWithError = new ArrayList<>();

        // Create an error-prone output field configuration, set to SKIP_ROW
        Column errorColumn =
                PhysicalColumn.of("error_field", BasicType.STRING_TYPE, 0, true, null, null);
        OutputFieldConfig errorConfig =
                new OutputFieldConfig(
                        "error_field",
                        errorColumn,
                        "items",
                        null,
                        "$.nonexistent.field",
                        null,
                        ErrorHandleWay.SKIP_ROW);
        outputFieldsWithError.add(errorConfig);

        FieldMappingInfo mappingInfo =
                new FieldMappingInfo(
                        inputRowType, outputFieldsWithError, FieldMergeStrategy.APPEND_NEW, null);

        // Create test data
        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {1L, "test", "{}"});
        Map<String, List<Object>> arrayDataMap = new HashMap<>();
        arrayDataMap.put("items", Arrays.asList(createJsonObject("item1", 100)));
        Map<String, Integer> arrayIndexMap = new HashMap<>();
        arrayIndexMap.put("items", 0);

        // Recreate converters
        JsonToRowConverters jsonToRowConverters = new JsonToRowConverters(false, false);
        JsonToRowConverters.JsonToObjectConverter[] newConverters =
                outputFieldsWithError.stream()
                        .map(OutputFieldConfig::getDestColumn)
                        .map(Column::getDataType)
                        .map(jsonToRowConverters::createConverter)
                        .toArray(JsonToRowConverters.JsonToObjectConverter[]::new);

        // Create ProcessedData object
        ProcessedData processedData = new ProcessedData(arrayDataMap, new HashMap<>());

        // Create output fields - should return null (skip entire row)
        Object[] outputFields =
                mappingInfo.createOutputFields(
                        inputRow, processedData, arrayIndexMap, newConverters, ErrorHandleWay.FAIL);

        // Verify result - should return null indicating skip entire row
        Assertions.assertNull(outputFields);
    }

    @Test
    public void testDataConversionError() {
        // Test data conversion error handling
        List<OutputFieldConfig> outputFieldsWithConversionError = new ArrayList<>();

        // Create a field with data type conversion error (string to number)
        Column intColumn = PhysicalColumn.of("int_field", BasicType.INT_TYPE, 0, true, null, null);
        OutputFieldConfig intConfig =
                new OutputFieldConfig(
                        "int_field",
                        intColumn,
                        "items",
                        null,
                        "$.name",
                        null,
                        ErrorHandleWay.SKIP); // name is string, converting to int will error
        outputFieldsWithConversionError.add(intConfig);

        FieldMappingInfo mappingInfo =
                new FieldMappingInfo(
                        inputRowType,
                        outputFieldsWithConversionError,
                        FieldMergeStrategy.APPEND_NEW,
                        null);

        // Create test data
        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {1L, "test", "{}"});
        Map<String, List<Object>> arrayDataMap = new HashMap<>();
        arrayDataMap.put("items", Arrays.asList(createJsonObject("item1", 100)));
        Map<String, Integer> arrayIndexMap = new HashMap<>();
        arrayIndexMap.put("items", 0);

        // Recreate converters
        JsonToRowConverters jsonToRowConverters = new JsonToRowConverters(false, false);
        JsonToRowConverters.JsonToObjectConverter[] newConverters =
                outputFieldsWithConversionError.stream()
                        .map(OutputFieldConfig::getDestColumn)
                        .map(Column::getDataType)
                        .map(jsonToRowConverters::createConverter)
                        .toArray(JsonToRowConverters.JsonToObjectConverter[]::new);

        // Create ProcessedData object
        ProcessedData processedData = new ProcessedData(arrayDataMap, new HashMap<>());

        // Create output fields - data conversion error should be skipped
        Object[] outputFields =
                mappingInfo.createOutputFields(
                        inputRow, processedData, arrayIndexMap, newConverters, ErrorHandleWay.FAIL);

        // Verify result - conversion error is skipped (filled with null)
        Assertions.assertEquals(4, outputFields.length);
        Assertions.assertEquals(1L, outputFields[0]); // id (source field)
        Assertions.assertEquals("test", outputFields[1]); // name (source field)
        Assertions.assertEquals("{}", outputFields[2]); // data (source field)
        Assertions.assertNull(outputFields[3]); // int_field (conversion error, skipped)
    }

    // Helper method: Create JSON object
    private Map<String, Object> createJsonObject(String name, int value) {
        Map<String, Object> obj = new HashMap<>();
        obj.put("name", name);
        obj.put("value", value);
        return obj;
    }
}
