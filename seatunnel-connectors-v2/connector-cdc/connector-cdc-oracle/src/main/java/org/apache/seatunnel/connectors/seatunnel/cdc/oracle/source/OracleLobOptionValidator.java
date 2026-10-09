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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.connectors.cdc.base.option.SourceOptions;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * Links Oracle LOB placeholder handling to Debezium {@code lob.enabled}.
 *
 * <p>Streaming rows are left unchanged unless {@code debezium.lob.enabled} is true and the user
 * turns on re-select or a non-default placeholder handling mode. Setting those enabling values
 * while LOB mining is off is rejected when the source is created, before any redo is read.
 */
public final class OracleLobOptionValidator {

    static final String LOB_ENABLED_KEY = "lob.enabled";

    private OracleLobOptionValidator() {}

    /**
     * Rejects enabling LOB options when Debezium will not mine LOB redo. Defaults are accepted and
     * do not change streaming behavior.
     */
    public static void validate(ReadonlyConfig config) {
        if (isDebeziumLobEnabled(config)) {
            return;
        }
        List<String> enabling = enablingOptions(config);
        if (enabling.isEmpty()) {
            return;
        }
        throw new OptionValidationException(
                String.format(
                        "Oracle CDC rejected %s because debezium.lob.enabled is not true. "
                                + "Streaming LOB rows stay unchanged unless LogMiner LOB mining is enabled. "
                                + "Set debezium.lob.enabled = \"true\" before lob.reselect.enabled=true "
                                + "or lob.unavailable-value.handling of null or fail. "
                                + "The defaults (lob.reselect.enabled=false, "
                                + "lob.unavailable-value.handling=warn_and_keep) do not change existing jobs.",
                        enabling));
    }

    /**
     * Whether the row converter should rewrite LOB placeholders. False leaves streaming rows
     * exactly as Debezium emitted them.
     */
    public static boolean placeholderHandlingActive(ReadonlyConfig config) {
        if (!isDebeziumLobEnabled(config)) {
            return false;
        }
        boolean reselect = config.get(OracleIncrementalSourceOptions.LOB_RESELECT_ENABLED);
        OracleLobUnavailableValueHandling handling =
                config.get(OracleIncrementalSourceOptions.LOB_UNAVAILABLE_VALUE_HANDLING);
        return reselect || handling != OracleLobUnavailableValueHandling.WARN_AND_KEEP;
    }

    static boolean isDebeziumLobEnabled(ReadonlyConfig config) {
        Optional<Map<String, String>> properties =
                config.getOptional(SourceOptions.DEBEZIUM_PROPERTIES);
        if (!properties.isPresent() || properties.get() == null) {
            return false;
        }
        String value = properties.get().get(LOB_ENABLED_KEY);
        return value != null && "true".equalsIgnoreCase(value.trim());
    }

    private static List<String> enablingOptions(ReadonlyConfig config) {
        List<String> enabling = new ArrayList<String>();
        Optional<Boolean> reselect =
                config.getOptional(OracleIncrementalSourceOptions.LOB_RESELECT_ENABLED);
        if (reselect.isPresent() && Boolean.TRUE.equals(reselect.get())) {
            enabling.add(OracleIncrementalSourceOptions.LOB_RESELECT_ENABLED.key() + "=true");
        }
        Optional<OracleLobUnavailableValueHandling> handling =
                config.getOptional(OracleIncrementalSourceOptions.LOB_UNAVAILABLE_VALUE_HANDLING);
        if (handling.isPresent()
                && handling.get() != OracleLobUnavailableValueHandling.WARN_AND_KEEP) {
            enabling.add(
                    OracleIncrementalSourceOptions.LOB_UNAVAILABLE_VALUE_HANDLING.key()
                            + "="
                            + handling.get().name().toLowerCase(Locale.ROOT));
        }
        return enabling;
    }
}
