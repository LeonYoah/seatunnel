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

import java.util.function.BooleanSupplier;

/**
 * Retries the Postgres WAL COPY stream after retriable disconnects without marking {@code
 * ChangeEventQueue} as failed.
 *
 * <p>Debezium {@code PostgresErrorHandler} would record a {@code RetriableException} on the queue.
 * SeaTunnel's split fetcher then fails the job on {@code queue.poll()}. This policy classifies the
 * same COPY/socket failures and lets {@link PostgresWalFetchTask} open a new replication connection
 * instead.
 */
final class PostgresWalStreamRetryPolicy {

    private static final String[] RETRIABLE_MESSAGE_FRAGMENTS = {
        "Database connection failed when reading from copy",
        "Database connection failed when writing to copy",
        "An I/O error occurred while sending to the backend",
        "This connection has been closed",
        "Socket is closed",
        "SSL",
        "unexpected message"
    };

    private PostgresWalStreamRetryPolicy() {}

    /**
     * Walks the cause chain and returns whether the failure is a retriable WAL COPY disconnect.
     *
     * @param throwable failure thrown by the streaming source
     * @return {@code true} when a retry should be attempted
     */
    static boolean isRetriable(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
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

    /**
     * Runs {@code streamAttempt} and retries retriable failures up to {@code maxRetries} times.
     *
     * <p>Does not call {@code ErrorHandler.setProducerThrowable}; the caller records the producer
     * exception only after this method throws.
     *
     * @param streamAttempt streaming attempt that throws on WAL failure
     * @param maxRetries retry budget from {@code connect.max-retries}
     * @param retryDelayMs Debezium {@code retryDelay()} in milliseconds. {@code 0} skips sleeping;
     *     a negative value uses {@code 1000L * attempt} milliseconds
     * @param running returns {@code false} when the fetch task has been shut down
     * @param retryListener invoked before sleeping and reopening the replication connection
     */
    static void executeWithRetry(
            StreamAttempt streamAttempt,
            int maxRetries,
            long retryDelayMs,
            BooleanSupplier running,
            RetryListener retryListener)
            throws Exception {
        int failures = 0;
        while (true) {
            try {
                streamAttempt.run();
                return;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw e;
            } catch (Exception e) {
                failures++;
                if (!running.getAsBoolean() || !isRetriable(e) || failures > maxRetries) {
                    throw e;
                }
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
    interface RetryListener {
        /**
         * Called after a retriable failure that will be retried.
         *
         * @param attempt 1-based retry count
         * @param maxRetries configured retry budget
         * @param failure last streaming failure
         */
        void onRetry(int attempt, int maxRetries, Throwable failure) throws Exception;
    }
}
