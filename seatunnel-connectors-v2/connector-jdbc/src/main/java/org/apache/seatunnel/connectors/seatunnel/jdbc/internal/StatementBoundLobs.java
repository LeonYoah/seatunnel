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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Clob;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/**
 * Temporary LOBs bound to a prepared statement. They stay alive until the batch executes, then
 * {@link #free(PreparedStatement)} releases them so Oracle does not keep the temporary LOBs.
 */
public final class StatementBoundLobs {

    private static final Logger LOG = LoggerFactory.getLogger(StatementBoundLobs.class);
    private static final Map<PreparedStatement, List<Clob>> BOUND =
            new IdentityHashMap<PreparedStatement, List<Clob>>();

    private StatementBoundLobs() {}

    public static void track(PreparedStatement statement, Clob clob) {
        synchronized (BOUND) {
            List<Clob> clobs = BOUND.get(statement);
            if (clobs == null) {
                clobs = new ArrayList<Clob>();
                BOUND.put(statement, clobs);
            }
            clobs.add(clob);
        }
    }

    public static void free(PreparedStatement statement) {
        List<Clob> clobs;
        synchronized (BOUND) {
            clobs = BOUND.remove(statement);
        }
        if (clobs == null) {
            return;
        }
        for (Clob clob : clobs) {
            try {
                clob.free();
            } catch (SQLException e) {
                LOG.warn("Failed to free a temporary JDBC LOB", e);
            }
        }
    }
}
