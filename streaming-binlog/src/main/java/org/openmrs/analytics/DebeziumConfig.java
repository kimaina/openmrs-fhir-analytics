// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.openmrs.analytics;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.relational.history.MemoryDatabaseHistory;


import java.util.function.Function;

/**
 * Debezium Connector configurator
 */
public class DebeziumConfig {

    private static final String APP_NAME = "DBZ";
    /**
     * database connector.
     *
     * @return Configuration.
     */

    public Configuration getConnectionConfig() {
        return Configuration.empty().withSystemProperties(Function.identity()).edit()
                .with(EmbeddedEngine.CONNECTOR_CLASS,  "io.debezium.connector.mysql.MySqlConnector")
                .with(EmbeddedEngine.ENGINE_NAME, APP_NAME)
                .with(MySqlConnectorConfig.SERVER_NAME,APP_NAME)
                .with(MySqlConnectorConfig.SERVER_ID, 8192)
                .with(EmbeddedEngine.OFFSET_STORAGE, "org.apache.kafka.connect.storage.FileOffsetBackingStore")
                .with(MySqlConnectorConfig.DATABASE_HISTORY, MemoryDatabaseHistory.class.getName())
                .with(EmbeddedEngine.OFFSET_STORAGE_FILE_FILENAME, "openmrs-offset.dat")
                .with(MySqlConnectorConfig.EVENT_PROCESSING_FAILURE_HANDLING_MODE, "warn")
               // .with(MySqlConnectorConfig.SNAPSHOT_MODE, "never")
                //.with("snapshot.new.tables", "parallel")
                .with("inconsistent.schema.handling.mode", "warn")
                .with("schemas.enable", false)

                .build();
    }
}