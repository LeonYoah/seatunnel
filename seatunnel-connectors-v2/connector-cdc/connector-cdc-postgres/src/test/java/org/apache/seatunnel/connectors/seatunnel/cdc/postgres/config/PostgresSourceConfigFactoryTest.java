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

package org.apache.seatunnel.connectors.seatunnel.cdc.postgres.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.cdc.base.config.StartupConfig;
import org.apache.seatunnel.connectors.cdc.base.config.StopConfig;
import org.apache.seatunnel.connectors.cdc.base.option.JdbcSourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.cdc.base.option.StopMode;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/** Tests the PG-base-backed PostgreSQL source config factory behavior that must stay compatible. */
public class PostgresSourceConfigFactoryTest {

    @Test
    public void testCreateFormatsSchemaQualifiedTableIdentifiers() {
        PostgresSourceConfigFactory factory = baseFactory();
        factory.tableList("inventory.orders", "db1.public.customers");

        PostgresSourceConfig sourceConfig = factory.create(0);

        Assertions.assertEquals(
                "inventory.orders,public.customers",
                sourceConfig.getDbzConfiguration().getString("table.include.list"));
    }

    @Test
    public void testCreateRejectsInvalidTableIdentifier() {
        PostgresSourceConfigFactory factory = baseFactory();
        factory.tableList("orders");

        IllegalArgumentException exception =
                Assertions.assertThrows(IllegalArgumentException.class, () -> factory.create(0));

        // Pin the wording: this message is user-facing and predates the PG-base extraction.
        Assertions.assertEquals(
                "Invalid table name: orders ,Postgres identifier is of the form schemaName.tableName",
                exception.getMessage());
    }

    @Test
    public void shouldDisableDebeziumSnapshotForCommittedOffsetStartup() {
        PostgresSourceConfigFactory factory = baseFactory();
        factory.startupOptions(new StartupConfig(StartupMode.COMMITTED_OFFSET, null, null, null));

        Assertions.assertEquals(
                "never", factory.create(0).getDbzConfiguration().getString("snapshot.mode"));
        // "database.include.list" must stay unset: Debezium turns it into a catalog predicate on
        // dataCollectionFilter(), which rejects the catalog-less TableIds used by PostgreSQL.
        Assertions.assertNull(
                factory.create(0).getDbzConfiguration().getString("database.include.list"));
    }

    @Test
    public void shouldRunDebeziumSnapshotOnlyForSnapshotOnlyStartup() {
        PostgresSourceConfigFactory factory = baseFactory();
        factory.startupOptions(new StartupConfig(StartupMode.SNAPSHOT_ONLY, null, null, null));

        Assertions.assertEquals(
                "initial_only", factory.create(0).getDbzConfiguration().getString("snapshot.mode"));
    }

    @Test
    public void shouldLeaveSnapshotModeUnsetForInitialStartup() {
        PostgresSourceConfigFactory factory = baseFactory();

        Assertions.assertNull(
                factory.create(0).getDbzConfiguration().getString("snapshot.mode"),
                "initial startup must keep the Debezium default snapshot mode");
    }

    @Test
    public void shouldKeepPostgresSpecificDebeziumProperties() {
        PostgresSourceConfigFactory factory = baseFactory();
        factory.tableList("inventory.orders");

        PostgresSourceConfig sourceConfig = factory.create(0);

        Assertions.assertEquals(
                "postgres_cdc_source",
                sourceConfig.getDbzConfiguration().getString("database.server.name"));
        Assertions.assertEquals(
                PostgresIncrementalSourceOptions.DECODING_PLUGIN_NAME.defaultValue(),
                sourceConfig.getDbzConfiguration().getString("plugin.name"));
        Assertions.assertEquals(
                PostgresIncrementalSourceOptions.SLOT_NAME.defaultValue(),
                sourceConfig.getDbzConfiguration().getString("slot.name"));
        Assertions.assertEquals("org.postgresql.Driver", sourceConfig.getDriverClassName());
    }

    @Test
    public void shouldLetUserDebeziumPropertiesOverrideDefaults() {
        PostgresSourceConfigFactory factory = baseFactory();
        Properties dbzProperties = new Properties();
        dbzProperties.setProperty("slot.name", "custom_slot");
        factory.debeziumProperties(dbzProperties);

        Assertions.assertEquals(
                "custom_slot", factory.create(0).getDbzConfiguration().getString("slot.name"));
    }

    /**
     * Pins the RELATION-message schema-evolution wiring: {@code include.schema.changes} must
     * reflect the actual {@code schema-changes.enabled} option instead of being hardcoded, since
     * Debezium PostgreSQL only emits the RELATION messages {@link PostgresIncrementalSource}'s
     * schema-change resolver depends on when this flag is set.
     */
    @Test
    public void shouldEnableSchemaChangesWhenSchemaEvolutionIsEnabled() {
        PostgresSourceConfigFactory factory = baseFactory();
        factory.schemaChangeEnabled(true);

        Assertions.assertEquals(
                "true",
                factory.create(0).getDbzConfiguration().getString("include.schema.changes"));
    }

    @Test
    public void shouldDisableSchemaChangesByDefault() {
        PostgresSourceConfigFactory factory = baseFactory();

        Assertions.assertEquals(
                "false",
                factory.create(0).getDbzConfiguration().getString("include.schema.changes"));
    }

    /**
     * SeaTunnel's own {@code schema-changes.enabled} option must stay authoritative even if a raw
     * {@code debezium.*} passthrough property also happens to set {@code include.schema.changes}.
     */
    @Test
    public void shouldKeepSchemaChangesEnabledAuthoritativeOverUserDebeziumProperties() {
        PostgresSourceConfigFactory factory = baseFactory();
        factory.schemaChangeEnabled(true);
        Properties dbzProperties = new Properties();
        dbzProperties.setProperty("include.schema.changes", "false");
        factory.debeziumProperties(dbzProperties);

        Assertions.assertEquals(
                "true",
                factory.create(0).getDbzConfiguration().getString("include.schema.changes"));
    }

    @Test
    public void shouldCopySslModeFromJdbcUrl() {
        PostgresSourceConfigFactory factory = baseFactory();
        factory.originUrl("jdbc:postgresql://127.0.0.1:5432/test?sslmode=disable");

        Assertions.assertEquals(
                "disable", factory.create(0).getDbzConfiguration().getString("database.sslmode"));
    }

    @Test
    public void shouldLetSslModeOptionWinOverJdbcUrl() {
        PostgresSourceConfigFactory factory =
                factoryFromReadonlyConfig(
                        option(PostgresIncrementalSourceOptions.SSL_MODE.key(), "require"));
        factory.originUrl("jdbc:postgresql://127.0.0.1:5432/test?sslmode=disable");

        Assertions.assertEquals(
                "require", factory.create(0).getDbzConfiguration().getString("database.sslmode"));
    }

    @Test
    public void shouldLeaveDatabaseSslModeUnsetWhenNeitherOptionNorUrlSetIt() {
        PostgresSourceConfigFactory factory = baseFactory();

        Assertions.assertNull(
                factory.create(0).getDbzConfiguration().getString("database.sslmode"));
    }

    @Test
    public void shouldWritePublicationOptionsWhenSet() {
        Map<String, Object> options = new HashMap<>();
        options.put(PostgresIncrementalSourceOptions.PUBLICATION_NAME.key(), "seatunnel_pub");
        options.put(PostgresIncrementalSourceOptions.PUBLICATION_AUTOCREATE_MODE.key(), "disabled");
        PostgresSourceConfigFactory factory = factoryFromReadonlyConfig(options);

        Assertions.assertEquals(
                "seatunnel_pub",
                factory.create(0).getDbzConfiguration().getString("publication.name"));
        Assertions.assertEquals(
                "disabled",
                factory.create(0).getDbzConfiguration().getString("publication.autocreate.mode"));
    }

    @Test
    public void shouldOmitPublicationKeysWhenUnset() {
        PostgresSourceConfigFactory factory = baseFactory();

        Assertions.assertNull(
                factory.create(0).getDbzConfiguration().getString("publication.name"));
        Assertions.assertNull(
                factory.create(0).getDbzConfiguration().getString("publication.autocreate.mode"));
    }

    @Test
    public void shouldKeepPublicationOptionsAuthoritativeOverUserDebeziumProperties() {
        Map<String, Object> options = new HashMap<>();
        options.put(PostgresIncrementalSourceOptions.PUBLICATION_NAME.key(), "seatunnel_pub");
        options.put(PostgresIncrementalSourceOptions.PUBLICATION_AUTOCREATE_MODE.key(), "filtered");
        PostgresSourceConfigFactory factory = factoryFromReadonlyConfig(options);
        Properties dbzProperties = new Properties();
        dbzProperties.setProperty("publication.name", "dbz_pub");
        dbzProperties.setProperty("publication.autocreate.mode", "all_tables");
        factory.debeziumProperties(dbzProperties);

        Assertions.assertEquals(
                "seatunnel_pub",
                factory.create(0).getDbzConfiguration().getString("publication.name"));
        Assertions.assertEquals(
                "filtered",
                factory.create(0).getDbzConfiguration().getString("publication.autocreate.mode"));
    }

    private PostgresSourceConfigFactory factoryFromReadonlyConfig(Map<String, Object> extra) {
        Map<String, Object> options = new HashMap<>();
        options.put(JdbcSourceOptions.USERNAME.key(), "user");
        options.put(JdbcSourceOptions.PASSWORD.key(), "pwd");
        options.putAll(extra);
        PostgresSourceConfigFactory factory = new PostgresSourceConfigFactory();
        factory.fromReadonlyConfig(ReadonlyConfig.fromMap(options));
        factory.hostname("127.0.0.1");
        factory.port(5432);
        factory.originUrl("jdbc:postgresql://127.0.0.1:5432/test");
        factory.databaseList("inventory");
        factory.startupOptions(new StartupConfig(StartupMode.INITIAL, null, null, null));
        factory.stopOptions(new StopConfig(StopMode.NEVER, null, null, null));
        return factory;
    }

    private static Map<String, Object> option(String key, String value) {
        Map<String, Object> options = new HashMap<>();
        options.put(key, value);
        return options;
    }

    private PostgresSourceConfigFactory baseFactory() {
        PostgresSourceConfigFactory factory = new PostgresSourceConfigFactory();
        factory.hostname("127.0.0.1");
        factory.port(5432);
        factory.username("user");
        factory.password("pwd");
        factory.originUrl("jdbc:postgresql://127.0.0.1:5432/test");
        factory.databaseList("inventory");
        factory.startupOptions(new StartupConfig(StartupMode.INITIAL, null, null, null));
        factory.stopOptions(new StopConfig(StopMode.NEVER, null, null, null));
        return factory;
    }
}
