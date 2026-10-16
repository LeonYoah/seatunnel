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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
        Assertions.assertTrue(
                PostgresWalStreamRetryPolicy.isRetriable(new SQLException("Connection reset")));
        Assertions.assertTrue(
                PostgresWalStreamRetryPolicy.isRetriable(
                        new SQLException("Connection reset by peer")));
    }

    @Test
    public void shouldNotRetryWhenRequestedWalHasBeenRemoved() {
        Assertions.assertFalse(
                PostgresWalStreamRetryPolicy.isRetriable(
                        new SQLException(
                                "ERROR: requested WAL segment 000000010000000000000000 has already been removed")));
        Assertions.assertFalse(
                PostgresWalStreamRetryPolicy.isRetriable(
                        new RuntimeException(
                                "could not access WAL after request",
                                new SQLException(
                                        "requested WAL segment has already been removed"))));
        Assertions.assertFalse(
                PostgresWalStreamRetryPolicy.isRetriable(
                        new SQLException("could not access WAL for requested LSN 0/0")));
        Assertions.assertFalse(
                PostgresWalStreamRetryPolicy.isRetriable(
                        new RuntimeException(
                                "An I/O error occurred while sending to the backend",
                                new SQLException(
                                        "requested WAL segment 000000010000000000000000 has already been removed"))));
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
    public void shouldGiveUpWhenFailuresExceedMaxRetriesOfOne() {
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
                                        1,
                                        0L,
                                        () -> true,
                                        (attempt, max, lastFailure) -> {}));

        Assertions.assertSame(failure, thrown);
        Assertions.assertEquals(2, runs.get());
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

    @Test
    public void shouldReopenJdbcBeforeReplication() throws Exception {
        List<String> order = new ArrayList<>();
        PostgresWalStreamRetryPolicy.reopenConnectionsOrContinue(
                () -> order.add("jdbc"), () -> order.add("replication"));
        Assertions.assertEquals(Arrays.asList("jdbc", "replication"), order);
    }

    @Test
    public void shouldContinueWhenJdbcReopenFailsWithRetriableIo() {
        AtomicInteger replicationRuns = new AtomicInteger();
        Assertions.assertDoesNotThrow(
                () ->
                        PostgresWalStreamRetryPolicy.reopenConnectionsOrContinue(
                                () -> {
                                    throw new SQLException("Connection reset");
                                },
                                replicationRuns::incrementAndGet));
        Assertions.assertEquals(0, replicationRuns.get());
    }

    @Test
    public void shouldThrowImmediatelyWhenJdbcReopenIsNotRetriable() {
        SQLException authFailure = new SQLException("password authentication failed");
        SQLException thrown =
                Assertions.assertThrows(
                        SQLException.class,
                        () ->
                                PostgresWalStreamRetryPolicy.reopenConnectionsOrContinue(
                                        () -> {
                                            throw authFailure;
                                        },
                                        () -> Assertions.fail("must not reopen replication")));
        Assertions.assertSame(authFailure, thrown);
    }

    @Test
    public void shouldThrowImmediatelyWhenReplicationReopenIsNotRetriable() {
        IllegalStateException slotFailure = new IllegalStateException("slot has been invalidated");
        IllegalStateException thrown =
                Assertions.assertThrows(
                        IllegalStateException.class,
                        () ->
                                PostgresWalStreamRetryPolicy.reopenConnectionsOrContinue(
                                        () -> {},
                                        () -> {
                                            throw slotFailure;
                                        }));
        Assertions.assertSame(slotFailure, thrown);
    }

    @Test
    public void shouldNotRetryWalRemovedDuringExecuteWithRetry() {
        AtomicInteger runs = new AtomicInteger();
        SQLException failure =
                new SQLException(
                        "requested WAL segment 000000010000000000000000 has already been removed");

        SQLException thrown =
                Assertions.assertThrows(
                        SQLException.class,
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
                                                        "WAL-removed errors must not be retried")));

        Assertions.assertSame(failure, thrown);
        Assertions.assertEquals(1, runs.get());
    }
}
