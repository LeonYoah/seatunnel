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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

/** Covers WAL COPY retry classification and budget without a live PostgreSQL instance. */
public class PostgresWalStreamRetryPolicyTest {

    @Test
    public void shouldTreatCopyDisconnectAsRetriable() {
        Assertions.assertTrue(
                PostgresWalStreamRetryPolicy.isRetriable(
                        new SQLException("Database connection failed when reading from copy")));
        Assertions.assertTrue(
                PostgresWalStreamRetryPolicy.isRetriable(
                        new SQLException("Database connection failed when writing to copy")));
        Assertions.assertTrue(
                PostgresWalStreamRetryPolicy.isRetriable(
                        new SQLException("An I/O error occurred while sending to the backend")));
        Assertions.assertTrue(
                PostgresWalStreamRetryPolicy.isRetriable(
                        new SQLException("This connection has been closed")));
        Assertions.assertTrue(
                PostgresWalStreamRetryPolicy.isRetriable(new RuntimeException("Socket is closed")));
        Assertions.assertTrue(
                PostgresWalStreamRetryPolicy.isRetriable(
                        new RuntimeException("SSL error: unexpected message")));
    }

    @Test
    public void shouldWalkCauseChainForRetriableFailures() {
        RuntimeException wrapped =
                new RuntimeException(
                        "streaming failed",
                        new SQLException("Database connection failed when reading from copy"));
        Assertions.assertTrue(PostgresWalStreamRetryPolicy.isRetriable(wrapped));
    }

    @Test
    public void shouldNotRetryNonRetriableFailures() {
        Assertions.assertFalse(
                PostgresWalStreamRetryPolicy.isRetriable(
                        new IllegalStateException("publication does not exist")));
    }

    @Test
    public void shouldGiveUpAfterMaxRetries() {
        AtomicInteger runs = new AtomicInteger();
        RuntimeException failure =
                new RuntimeException(
                        new SQLException("Database connection failed when reading from copy"));

        RuntimeException thrown =
                Assertions.assertThrows(
                        RuntimeException.class,
                        () ->
                                PostgresWalStreamRetryPolicy.executeWithRetry(
                                        () -> {
                                            runs.incrementAndGet();
                                            throw failure;
                                        },
                                        3,
                                        0L,
                                        () -> true,
                                        (attempt, max, lastFailure) -> {}));

        Assertions.assertSame(failure, thrown);
        Assertions.assertEquals(4, runs.get());
    }

    @Test
    public void shouldSucceedAfterRetriableFailure() throws Exception {
        AtomicInteger runs = new AtomicInteger();
        RuntimeException failure =
                new RuntimeException(new SQLException("This connection has been closed"));

        Assertions.assertDoesNotThrow(
                () ->
                        PostgresWalStreamRetryPolicy.executeWithRetry(
                                () -> {
                                    if (runs.incrementAndGet() == 1) {
                                        throw failure;
                                    }
                                },
                                3,
                                0L,
                                () -> true,
                                (attempt, max, lastFailure) -> {}));
        Assertions.assertEquals(2, runs.get());
    }

    @Test
    public void shouldNotRetryWhenTaskIsStopped() {
        AtomicInteger runs = new AtomicInteger();
        RuntimeException failure = new RuntimeException(new SQLException("Socket is closed"));

        RuntimeException thrown =
                Assertions.assertThrows(
                        RuntimeException.class,
                        () ->
                                PostgresWalStreamRetryPolicy.executeWithRetry(
                                        () -> {
                                            runs.incrementAndGet();
                                            throw failure;
                                        },
                                        3,
                                        0L,
                                        () -> false,
                                        (attempt, max, lastFailure) ->
                                                Assertions.fail(
                                                        "retry must not run when stopped")));

        Assertions.assertSame(failure, thrown);
        Assertions.assertEquals(1, runs.get());
    }

    @Test
    public void shouldFailImmediatelyForNonRetriableError() {
        AtomicInteger runs = new AtomicInteger();
        IllegalStateException failure = new IllegalStateException("slot has been invalidated");

        IllegalStateException thrown =
                Assertions.assertThrows(
                        IllegalStateException.class,
                        () ->
                                PostgresWalStreamRetryPolicy.executeWithRetry(
                                        () -> {
                                            runs.incrementAndGet();
                                            throw failure;
                                        },
                                        3,
                                        0L,
                                        () -> true,
                                        (attempt, max, lastFailure) ->
                                                Assertions.fail(
                                                        "non-retriable errors must not be retried")));

        Assertions.assertSame(failure, thrown);
        Assertions.assertEquals(1, runs.get());
    }
}
