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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.connector.postgresql.connection.Lsn;

import java.sql.SQLException;

/** Covers earliest 0/0 fail-stop wrapping without a live PostgreSQL instance. */
public class PostgresWalStartLsnUnavailableTest {

    @Test
    public void shouldTreatInvalidLsnAsUnavailableStart() {
        Assertions.assertTrue(
                PostgresWalStartLsnUnavailable.isUnavailableStartLsn(Lsn.INVALID_LSN));
        Assertions.assertTrue(PostgresWalStartLsnUnavailable.isUnavailableStartLsn(null));
        Assertions.assertTrue(
                PostgresWalStartLsnUnavailable.isUnavailableStartLsn(Lsn.valueOf("0/0")));
        Assertions.assertFalse(
                PostgresWalStartLsnUnavailable.isUnavailableStartLsn(Lsn.valueOf("0/16B6C50")));
    }

    @Test
    public void shouldFormatInvalidLsnAsZeroSlashZero() {
        Assertions.assertEquals("0/0", PostgresWalStartLsnUnavailable.formatLsn(null));
        Assertions.assertEquals("0/0", PostgresWalStartLsnUnavailable.formatLsn(Lsn.INVALID_LSN));
        Assertions.assertEquals(
                "0/16B6C50", PostgresWalStartLsnUnavailable.formatLsn(Lsn.valueOf("0/16B6C50")));
    }

    @Test
    public void shouldWrapWalRemovedWithCommittedOffsetGuidance() {
        SQLException cause =
                new SQLException(
                        "ERROR: requested WAL segment 000000010000000000000000 has already been removed");

        Exception wrapped =
                PostgresWalStartLsnUnavailable.wrapIfNeeded(cause, "0/0", "seatunnel_slot");

        Assertions.assertInstanceOf(SeaTunnelRuntimeException.class, wrapped);
        SeaTunnelRuntimeException exception = (SeaTunnelRuntimeException) wrapped;
        Assertions.assertEquals(
                PostgresConnectorErrorCode.WAL_START_LSN_UNAVAILABLE,
                exception.getSeaTunnelErrorCode());
        Assertions.assertTrue(exception.getMessage().contains("0/0"));
        Assertions.assertTrue(exception.getMessage().contains("seatunnel_slot"));
        Assertions.assertTrue(exception.getMessage().contains("committed-offset"));
        Assertions.assertTrue(exception.getMessage().contains("latest"));
        Assertions.assertTrue(exception.getMessage().contains("initial"));
        Assertions.assertTrue(exception.getMessage().contains("restart_lsn"));
        Assertions.assertTrue(
                exception.getMessage().contains("startup.mode=earliest requests LSN 0/0"));
        Assertions.assertSame(cause, exception.getCause());
    }

    @Test
    public void shouldNotWrapRetriableIoAsWalUnavailable() {
        SQLException io = new SQLException("An I/O error occurred while sending to the backend");
        Exception wrapped = PostgresWalStartLsnUnavailable.wrapIfNeeded(io, "0/16B6C50", "slot");
        Assertions.assertSame(io, wrapped);
    }

    @Test
    public void shouldNotMentionEarliestWhenNonZeroLsnWalWasRemoved() {
        SQLException cause =
                new SQLException(
                        "ERROR: requested WAL segment 000000010000000000000016 has already been removed");

        Exception wrapped =
                PostgresWalStartLsnUnavailable.wrapIfNeeded(cause, "0/16B6C50", "seatunnel_slot");

        Assertions.assertInstanceOf(SeaTunnelRuntimeException.class, wrapped);
        Assertions.assertTrue(wrapped.getMessage().contains("0/16B6C50"));
        Assertions.assertTrue(wrapped.getMessage().contains("seatunnel_slot"));
        Assertions.assertFalse(
                wrapped.getMessage().contains("startup.mode=earliest requests LSN 0/0"));
    }

    @Test
    public void shouldNotDoubleWrapAlreadyUnavailableException() {
        SeaTunnelRuntimeException original =
                PostgresWalStartLsnUnavailable.unavailable("0/0", "slot_a");
        Exception wrapped = PostgresWalStartLsnUnavailable.wrapIfNeeded(original, "0/0", "slot_a");
        Assertions.assertSame(original, wrapped);
    }
}
