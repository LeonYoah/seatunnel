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

package org.apache.seatunnel.connectors.seatunnel.cdc.postgres.source.reader.wal;

import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.exception.PostgresConnectorErrorCode;

import io.debezium.connector.postgresql.connection.Lsn;

/**
 * Builds the fail-stop error used when PostgreSQL cannot start logical decoding from the requested
 * LSN.
 *
 * <p>Does not clamp to {@code restart_lsn}. {@code startup.mode=earliest} still requests LSN {@code
 * 0/0}.
 */
public final class PostgresWalStartLsnUnavailable {

    private static final String[] WAL_UNAVAILABLE_FRAGMENTS = {
        "requested WAL segment", "has already been removed", "could not access WAL"
    };

    private PostgresWalStartLsnUnavailable() {}

    /**
     * Returns whether {@code lsn} is missing or the invalid 0/0 pointer that PostgreSQL never
     * stores a record at.
     */
    public static boolean isUnavailableStartLsn(Lsn lsn) {
        return lsn == null || !lsn.isValid();
    }

    /**
     * Formats an LSN for operator-facing messages. Invalid or null values are shown as {@code 0/0}.
     */
    public static String formatLsn(Lsn lsn) {
        if (isUnavailableStartLsn(lsn)) {
            return "0/0";
        }
        return lsn.asString();
    }

    /**
     * Walks the cause chain for PostgreSQL recycled-WAL fragments.
     *
     * @param throwable failure thrown while starting or reading WAL
     * @return {@code true} when the LSN/WAL is gone and must not be retried
     */
    public static boolean isWalStartLsnUnavailable(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            if (current instanceof SeaTunnelRuntimeException) {
                if (PostgresConnectorErrorCode.WAL_START_LSN_UNAVAILABLE
                        == ((SeaTunnelRuntimeException) current).getSeaTunnelErrorCode()) {
                    return true;
                }
            }
            String message = current.getMessage();
            if (message != null) {
                for (String fragment : WAL_UNAVAILABLE_FRAGMENTS) {
                    if (message.contains(fragment)) {
                        return true;
                    }
                }
            }
            current = current.getCause();
        }
        return false;
    }

    /**
     * Wraps {@code cause} when it is a recycled-WAL failure; otherwise returns {@code cause} if it
     * is already an {@link Exception}.
     */
    public static Exception wrapIfNeeded(Throwable cause, String requestedLsn, String slotName) {
        if (cause instanceof SeaTunnelRuntimeException
                && PostgresConnectorErrorCode.WAL_START_LSN_UNAVAILABLE
                        == ((SeaTunnelRuntimeException) cause).getSeaTunnelErrorCode()) {
            return (SeaTunnelRuntimeException) cause;
        }
        if (isWalStartLsnUnavailable(cause)) {
            return unavailable(requestedLsn, slotName, cause);
        }
        if (cause instanceof Exception) {
            return (Exception) cause;
        }
        return new RuntimeException(cause);
    }

    /**
     * Fail-stop exception for a requested start LSN that PostgreSQL cannot decode from.
     *
     * @param requestedLsn LSN the job asked to start from ({@code 0/0} for earliest)
     * @param slotName replication slot name
     */
    public static SeaTunnelRuntimeException unavailable(String requestedLsn, String slotName) {
        return unavailable(requestedLsn, slotName, null);
    }

    /**
     * Fail-stop exception for a requested start LSN that PostgreSQL cannot decode from.
     *
     * @param requestedLsn LSN the job asked to start from ({@code 0/0} for earliest)
     * @param slotName replication slot name
     * @param cause original PostgreSQL/Debezium failure, if any
     */
    public static SeaTunnelRuntimeException unavailable(
            String requestedLsn, String slotName, Throwable cause) {
        String lsnText = requestedLsn == null || requestedLsn.isEmpty() ? "0/0" : requestedLsn;
        String slotText = slotName == null || slotName.isEmpty() ? "<unknown>" : slotName;
        String earliestHint =
                "0/0".equals(lsnText) ? "startup.mode=earliest requests LSN 0/0. " : "";
        String message =
                "PostgreSQL logical decoding cannot start from requested LSN "
                        + lsnText
                        + " for replication slot '"
                        + slotText
                        + "'. PostgreSQL cannot start logical decoding from a recycled LSN or from a position before the slot restart_lsn. "
                        + earliestHint
                        + "Use startup.mode=committed-offset with an existing slot, or latest / initial.";
        if (cause == null) {
            return new SeaTunnelRuntimeException(
                    PostgresConnectorErrorCode.WAL_START_LSN_UNAVAILABLE, message);
        }
        return new SeaTunnelRuntimeException(
                PostgresConnectorErrorCode.WAL_START_LSN_UNAVAILABLE, message, cause);
    }
}
