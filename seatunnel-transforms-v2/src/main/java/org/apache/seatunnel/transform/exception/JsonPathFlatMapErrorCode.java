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
package org.apache.seatunnel.transform.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

public enum JsonPathFlatMapErrorCode implements SeaTunnelErrorCode {
    ARRAY_FLATTEN_FIELDS_EMPTY(
            "JSONPATH_FLATMAP_ERROR_CODE-01",
            "JsonPathFlatMap array_flatten_fields must not be empty"),

    OUTPUT_FIELDS_EMPTY(
            "JSONPATH_FLATMAP_ERROR_CODE-02", "JsonPathFlatMap output_fields must not be empty"),

    JSON_PATH_COMPILE_ERROR(
            "JSONPATH_FLATMAP_ERROR_CODE-03", "JsonPathFlatMap JsonPath compilation failed"),

    JSON_UNESCAPE_ERROR("JSONPATH_FLATMAP_ERROR_CODE-04", "JsonPathFlatMap JSON unescape failed"),

    ARRAY_EXTRACTION_ERROR(
            "JSONPATH_FLATMAP_ERROR_CODE-05", "JsonPathFlatMap array extraction failed"),

    TRANSFORM_EXECUTION_ERROR(
            "JSONPATH_FLATMAP_ERROR_CODE-06", "JsonPathFlatMap transform execution failed"),

    SRC_FIELD_NOT_FOUND(
            "JSONPATH_FLATMAP_ERROR_CODE-07",
            "JsonPathFlatMap source field not found in input row"),

    OUTPUT_FIELD_EXTRACTION_ERROR(
            "JSONPATH_FLATMAP_ERROR_CODE-08", "JsonPathFlatMap output field extraction failed"),

    DATA_CONVERSION_ERROR(
            "JSONPATH_FLATMAP_ERROR_CODE-09", "JsonPathFlatMap data conversion failed");

    private final String code;
    private final String description;

    JsonPathFlatMapErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return code;
    }

    @Override
    public String getDescription() {
        return description;
    }
}
