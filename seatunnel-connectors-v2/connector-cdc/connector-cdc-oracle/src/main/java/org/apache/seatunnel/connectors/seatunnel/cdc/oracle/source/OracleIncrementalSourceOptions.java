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

package org.apache.seatunnel.connectors.seatunnel.cdc.oracle.source;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.SingleChoiceOption;
import org.apache.seatunnel.connectors.cdc.base.option.JdbcSourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.SourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.cdc.base.option.StopMode;

import java.util.Arrays;
import java.util.List;

public class OracleIncrementalSourceOptions extends JdbcSourceOptions {
    public static final SingleChoiceOption<StartupMode> STARTUP_MODE =
            (SingleChoiceOption)
                    Options.key(SourceOptions.STARTUP_MODE_KEY)
                            .singleChoice(
                                    StartupMode.class,
                                    Arrays.asList(
                                            StartupMode.INITIAL,
                                            StartupMode.LATEST,
                                            StartupMode.SPECIFIC,
                                            StartupMode.TIMESTAMP))
                            .defaultValue(StartupMode.INITIAL)
                            .withDescription(
                                    "Optional startup mode for CDC source, valid enumerations are "
                                            + "\"initial\", \"latest\", \"specific\" or \"timestamp\"");

    public static final SingleChoiceOption<StopMode> STOP_MODE =
            (SingleChoiceOption)
                    Options.key(SourceOptions.STOP_MODE_KEY)
                            .singleChoice(StopMode.class, Arrays.asList(StopMode.NEVER))
                            .defaultValue(StopMode.NEVER)
                            .withDescription(
                                    "Optional stop mode for CDC source, valid enumerations are "
                                            + "\"never\"");

    public static final Option<List<String>> SCHEMA_NAMES =
            Options.key("schema-names")
                    .listType()
                    .noDefaultValue()
                    .withDescription("Schema name of the database to monitor.");

    public static final Option<Long> STARTUP_SPECIFIC_OFFSET_SCN =
            Options.key("startup.specific-offset.scn")
                    .longType()
                    .noDefaultValue()
                    .withDescription("Optional SCN used in case of \"specific\" startup mode.");

    public static final Option<Boolean> USE_SELECT_COUNT =
            Options.key("use_select_count")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Use select count for table count in full stage");

    public static final Option<Boolean> SKIP_ANALYZE =
            Options.key("skip_analyze")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Skip the analysis of table count in full stage");

    public static final Option<Boolean> LOB_RESELECT_ENABLED =
            Options.key("lob.reselect.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "When debezium.lob.enabled is true, re-select CLOB, NCLOB, and BLOB columns whose"
                                    + " INSERT or UPDATE_AFTER value is Debezium's unavailable-value placeholder."
                                    + " The lookup uses the primary key and binds commit_scn as AS OF SCN ?."
                                    + " DELETE and UPDATE_BEFORE placeholders are replaced with null. Requires"
                                    + " SELECT and FLASHBACK ANY TABLE, or FLASHBACK on the table. Ignored unless"
                                    + " debezium.lob.enabled is true. Setting true without that flag is rejected"
                                    + " at startup. A re-select failure follows lob.unavailable-value.handling and"
                                    + " does not fail the job unless that option is fail.");

    public static final Option<OracleLobUnavailableValueHandling> LOB_UNAVAILABLE_VALUE_HANDLING =
            Options.key("lob.unavailable-value.handling")
                    .enumType(OracleLobUnavailableValueHandling.class)
                    .defaultValue(OracleLobUnavailableValueHandling.WARN_AND_KEEP)
                    .withDescription(
                            "What to do when a LOB unavailable-value placeholder cannot be re-selected, and only"
                                    + " when debezium.lob.enabled is true. warn_and_keep logs once and keeps the"
                                    + " placeholder. null replaces it with null. fail stops the task. Re-select"
                                    + " errors use the same choice and fail the job only when this option is fail."
                                    + " null and fail are rejected at startup unless debezium.lob.enabled is true.");
}
