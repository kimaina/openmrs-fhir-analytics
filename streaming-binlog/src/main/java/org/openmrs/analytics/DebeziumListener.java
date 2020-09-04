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
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.engine.DebeziumEngine;
import io.debezium.util.Clock;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static io.debezium.data.Envelope.FieldName.*;
import static java.util.stream.Collectors.toMap;

/**
 * Debezium change data capture / Listener
 */
public class DebeziumListener implements Runnable {

    /**
     * The Debezium engine controlling the app lifecycle
     */
    private final EmbeddedEngine engine;

    /**
     * Others
     */
    private static final Logger log = LoggerFactory.getLogger(DebeziumListener.class);
    private final JsonConverter valueConverter;

    /**
     * @apiNote Constructor which loads the dbz config then sets even handler 'handleEvent', which is invoked
     * whenever there is a change in database
     * @param debeziumConfig
     */
    public DebeziumListener(Configuration debeziumConfig) {
        this.engine = EmbeddedEngine
                .create()
                .using(debeziumConfig)
                .using(Clock.SYSTEM)
                .notifying(this::handleEvent)
                .build();

        valueConverter = new JsonConverter();
        valueConverter.configure(debeziumConfig.asMap(), false);

    }

    @Override
    public void run() {

        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.execute(engine);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Requesting embedded engine to shut down");
            engine.stop();
        }));

        // the submitted task keeps running, only no more new ones can be added
        //executor.shutdown();

        //awaitTermination(executor);

        cleanUp();

        log.info("Engine terminated");
    }


    /**
     * invoked when CRUD is performed on any whitelisted tables
     *
     * @param sourceRecord
     */
    private void handleEvent1(SourceRecord sourceRecord) {
        try{
            Struct sourceRecordValue = (Struct) sourceRecord.value();
            log.info("Data Changed: {} ", sourceRecordValue);
            if(sourceRecordValue != null) {
                DbOperation operation = DbOperation.forCode((String) sourceRecordValue.get(OPERATION));

                //Only if this is a transactional operation.
                if(operation != DbOperation.READ) {

                    Map<String, Object> message;
                    String record = AFTER; //For Update & Insert operations.

                    if (operation == DbOperation.DELETE) {
                        record = BEFORE; //For Delete operations.
                    }

                    //Build a map with all row data received.
                    Struct struct = (Struct) sourceRecordValue.get(record);
                    message = struct.schema().fields().stream()
                            .map(Field::name)
                            .filter(fieldName -> struct.get(fieldName) != null)
                            .map(fieldName -> Pair.of(fieldName, struct.get(fieldName)))
                            .collect(toMap(Pair::getKey, Pair::getValue));

                    log.info("Data Changed: {} with Operation: {}", message, operation.name());
                }
            }
        } catch (Exception ex) {
            log.info("exception in handle event:" + ex);
        }
    }

    private void handleEvent(SourceRecord record )  {

            log.info("Event Fired with record: {}", record.topic().toString());
            // We are interested only in data events not schema change events
            if (record.topic().equals("DBZ")) {
                log.info("Event Returning !!!!" );
                return;
            }

            Schema schema = null;

            if (null == record.keySchema()) {
                log.error("The keySchema is missing. Something is wrong.");
                return;
            }

            // For deletes, the value node is null
            if (null != record.valueSchema()) {
                schema = SchemaBuilder.struct()
                        .field("key", record.keySchema())
                        .field("value", record.valueSchema())
                        .build();
            } else {
                schema = SchemaBuilder.struct()
                        .field("key", record.keySchema())
                        .build();
            }

            Struct message = new Struct(schema);
            message.put("key", record.key());

            if (null != record.value())
                message.put("value", record.value());

            String partitionKey = String.valueOf(record.key() != null ? record.key().hashCode() : -1);
            final byte[] payload = valueConverter.fromConnectData("dummy", schema, message);

            log.info("Data Changed: {} with payload: {}", message.toString(), payload.toString());
            //DebeziumEngine.RecordCommitter committer.markProcessed(record);

    }


    private void awaitTermination(ExecutorService executor) {
        try {
            while (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                log.info("Waiting another 10 seconds for the embedded engine to complete");
            }
        }
        catch (InterruptedException e) {
             Thread.currentThread().interrupt();
        }
    }

    private void cleanUp() {
        log.info("Clean up process");
        // TODO
    }

}
