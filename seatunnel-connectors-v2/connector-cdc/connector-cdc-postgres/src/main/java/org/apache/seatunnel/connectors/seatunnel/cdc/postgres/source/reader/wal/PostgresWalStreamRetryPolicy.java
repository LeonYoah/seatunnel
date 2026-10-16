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

import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;

import java.io.EOFException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

/**
 * Retries the Postgres WAL COPY stream after retriable disconnects without marking {@code
 * ChangeEventQueue} as failed.
 *
 * <p>Debezium {@code PostgresErrorHandler} would record a {@code RetriableException} on the queue.
 * SeaTunnel's split fetcher then fails the job on {@code queue.poll()}. This policy classifies the
 * same COPY/socket failures and lets {@link PostgresWalFetchTask} reopen the JDBC {@code
 * dataConnection} in place and then a new replication connection.
 *
 * <p>Recycled-WAL / requested-LSN-unavailable failures are never retried. The retry budget is
 * {@code connect.max-retries} consecutive failures and resets once a reconnected stream completes a
 * healthy read cycle.
 */
public final class PostgresWalStreamRetryPolicy {

    private static final String[] RETRIABLE_MESSAGE_FRAGMENTS = {
        "Database connection failed when reading from copy",
        "Database connection failed when writing to copy",
        "An I/O error occurred while sending to the backend",
        "This connection has been closed",
        "This replication stream has been closed",
        "terminating connection due to administrator command",
        "terminating connection due to idle-session timeout",
        "terminating connection because of crash of another server process",
        "the database system is starting up",
        "the database system is shutting down",
        "Socket is closed",
        "Connection reset",
        "Broken pipe",
        "EOFException",
        "SSL error:",
        "SSL syscall error",
        "SSL peer shut down incorrectly"
    };

    private PostgresWalStreamRetryPolicy() {}

    /**
     * Walks the cause chain and returns whether the failure is a retriable WAL COPY or JDBC
     * disconnect.
     *
     * <p>Requested-WAL-unavailable / recycled-WAL errors are never retriable, even if another
     * fragment in the chain would otherwise match.
     *
     * @param throwable failure thrown by the streaming source
     * @return {@code true} when a retry should be attempted
     */
    public static boolean isRetriable(Throwable throwable) {
        if (PostgresWalStartLsnUnavailable.isWalStartLsnUnavailable(throwable)) {
            return false;
        }
        Throwable current = throwable;
        while (current != null) {
            if (isRetriableExceptionType(current)) {
                return true;
            }
            String message = current.getMessage();
            if (message != null) {
                for (String fragment : RETRIABLE_MESSAGE_FRAGMENTS) {
                    if (message.contains(fragment)) {
                        return true;
                    }
                }
            }
            current = current.getCause();
        }
        return false;
    }

    private static boolean isRetriableExceptionType(Throwable throwable) {
        if (throwable instanceof SocketException
                || throwable instanceof SocketTimeoutException
                || throwable instanceof EOFException) {
            return true;
        }
        return throwable instanceof SSLException && !(throwable instanceof SSLHandshakeException);
    }

    /**
     * Runs {@code streamAttempt} and retries retriable failures up to {@code maxRetries}
     * consecutive times.
     *
     * @param streamAttempt streaming attempt that throws on WAL failure
     * @param maxRetries retry budget from {@code connect.max-retries}
     * @param retryDelayMs Debezium {@code retryDelay()} in milliseconds. {@code 0} skips sleeping;
     *     a negative value uses {@code 1000L * attempt} milliseconds
     * @param running returns {@code false} when the fetch task has been shut down
     * @param retryListener invoked when a retriable failure occurs
     */
    static void executeWithRetry(
            StreamAttempt streamAttempt,
            int maxRetries,
            long retryDelayMs,
            BooleanSupplier running,
            RetryListener retryListener)
            throws Exception {
        executeWithRetry(
                markHealthy -> streamAttempt.run(),
                () -> {},
                maxRetries,
                retryDelayMs,
                running,
                retryListener);
    }

    /**
     * Runs {@code streamAttempt} and retries retriable failures up to {@code maxRetries}
     * consecutive times, sleeping before invoking {@code reconnectAttempt} inside the retry loop.
     *
     * <p>Running {@code reconnectAttempt} after the backoff delay and inside the {@code try} block
     * ensures that a transient JDBC or replication reconnect failure is also classified and retried
     * under the same {@code maxRetries} budget instead of leaving {@code replicationConnection} as
     * {@code null}.
     *
     * @param streamAttempt streaming attempt receiving a callback to reset the consecutive failure
     *     counter once streaming is healthy
     * @param reconnectAttempt reopens JDBC and replication connections before a retry stream
     *     attempt
     * @param maxRetries consecutive retry budget from {@code connect.max-retries}
     * @param retryDelayMs Debezium {@code retryDelay()} in milliseconds. {@code 0} skips sleeping;
     *     a negative value uses {@code 1000L * attempt} milliseconds
     * @param running returns {@code false} when the fetch task has been shut down
     * @param retryListener invoked before sleeping when a retriable failure will be retried
     */
    static void executeWithRetry(
            HealthyStreamAttempt streamAttempt,
            StreamAttempt reconnectAttempt,
            int maxRetries,
            long retryDelayMs,
            BooleanSupplier running,
            RetryListener retryListener)
            throws Exception {
        AtomicInteger consecutiveFailures = new AtomicInteger();
        boolean needsReconnect = false;
        while (true) {
            try {
                if (needsReconnect) {
                    reconnectAttempt.run();
                }
                streamAttempt.run(() -> consecutiveFailures.set(0));
                return;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw e;
            } catch (Exception e) {
                int failures = consecutiveFailures.incrementAndGet();
                if (!running.getAsBoolean() || !isRetriable(e) || failures > maxRetries) {
                    throw e;
                }
                needsReconnect = true;
                retryListener.onRetry(failures, maxRetries, e);
                // Positive delay uses Debezium retryDelay(); 0 skips sleep (tests); negative
                // falls back to 1000L * attempt milliseconds.
                long delayMs = retryDelayMs >= 0 ? retryDelayMs : 1000L * failures;
                if (delayMs > 0) {
                    Thread.sleep(delayMs);
                }
            }
        }
    }

    @FunctionalInterface
    interface StreamAttempt {
        void run() throws Exception;
    }

    @FunctionalInterface
    interface HealthyStreamAttempt {
        void run(Runnable markHealthy) throws Exception;
    }

    @FunctionalInterface
    interface RetryListener {
        /**
         * Called after a retriable failure that will be retried.
         *
         * @param attempt 1-based consecutive retry count
         * @param maxRetries configured retry budget
         * @param failure last streaming or reconnect failure
         */
        void onRetry(int attempt, int maxRetries, Throwable failure) throws Exception;
    }
}
