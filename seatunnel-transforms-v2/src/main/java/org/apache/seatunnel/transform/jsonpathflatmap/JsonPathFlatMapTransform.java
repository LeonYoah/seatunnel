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

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.transform.common.AbstractCatalogSupportFlatMapTransform;
import org.apache.seatunnel.transform.exception.ErrorDataTransformException;

import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;

import static org.apache.seatunnel.transform.exception.JsonPathFlatMapErrorCode.TRANSFORM_EXECUTION_ERROR;

@Slf4j
public class JsonPathFlatMapTransform extends AbstractCatalogSupportFlatMapTransform {

    public static final String PLUGIN_NAME = "JsonPathFlatMap";

    private final JsonPathFlatMapConfig config;
    private final JsonPathProcessor processor;
    private final RowBuilder rowBuilder;

    public JsonPathFlatMapTransform(JsonPathFlatMapConfig config, CatalogTable catalogTable) {
        super(catalogTable, config.getErrorHandleWay());
        this.config = config;
        this.processor = new JsonPathProcessor(config, catalogTable.getSeaTunnelRowType());
        this.rowBuilder = new RowBuilder(config, catalogTable.getSeaTunnelRowType());
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    protected List<SeaTunnelRow> transformRow(SeaTunnelRow inputRow) {
        ProcessedData processedData = processor.extractAllData(inputRow);
        if (processedData.isEmpty()) {
            return Collections.emptyList();
        }
        return rowBuilder.buildRows(inputRow, processedData);
    }

    @Override
    protected TableSchema transformTableSchema() {
        return rowBuilder.createOutputTableSchema(inputCatalogTable.getTableSchema());
    }

    @Override
    protected TableIdentifier transformTableIdentifier() {
        return inputCatalogTable.getTableId().copy();
    }
}
