/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rest.server.apiactions;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.api.ApiConnection;
import io.confluent.ksql.api.server.actions.InsertAction;
import io.confluent.ksql.api.server.actions.Inserter;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.logging.processing.NoopProcessingLogContext;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.serde.GenericKeySerDe;
import io.confluent.ksql.serde.GenericRowSerDe;
import io.confluent.ksql.serde.KeySerdeFactory;
import io.confluent.ksql.serde.ValueSerdeFactory;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.vertx.core.json.JsonObject;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;

public class ServerInsertAction extends InsertAction {

  private final KsqlEngine ksqlEngine;
  private final KsqlConfig ksqlConfig;

  public ServerInsertAction(int channelID, ApiConnection apiConnection,
      KsqlEngine ksqlEngine, KsqlConfig ksqlConfig) {
    super(channelID, apiConnection);
    this.ksqlEngine = ksqlEngine;
    this.ksqlConfig = ksqlConfig;
  }

  @Override
  protected Inserter createInserter(Integer channelID, String target) {
    return new ServerInserter(channelID, target);
  }

  private class ServerInserter implements Inserter {

    private Producer<byte[], byte[]> producer;
    private Serializer<Struct> keySerializer;
    private Serializer<GenericRow> valueSerializer;
    private DataSource<?> dataSource;
    private String topicName;

    ServerInserter(int channelID, String target) {

      ServiceContext serviceContext = ksqlEngine.getServiceContext();

      dataSource = ksqlEngine
          .getMetaStore()
          .getSource(SourceName.of(target));

      if (dataSource == null) {
        apiConnection.handleError(channelID, "Cannot insert values into an unknown stream/table: "
            + target);
        return;
      }

      if (dataSource.getKsqlTopic().getKeyFormat().isWindowed()) {
        apiConnection.handleError(channelID, "Cannot insert values into windowed stream/table!");
        return;
      }

      topicName = dataSource.getKafkaTopicName();

      final PhysicalSchema physicalSchema = PhysicalSchema.from(
          dataSource.getSchema(),
          dataSource.getSerdeOptions()
      );

      KeySerdeFactory keySerdeFactory = new GenericKeySerDe();

      keySerializer = keySerdeFactory.create(
          dataSource.getKsqlTopic().getKeyFormat().getFormatInfo(),
          physicalSchema.keySchema(),
          ksqlConfig,
          serviceContext.getSchemaRegistryClientFactory(),
          "",
          NoopProcessingLogContext.INSTANCE
      ).serializer();

      ValueSerdeFactory valueSerdeFactory = new GenericRowSerDe();

      valueSerializer = valueSerdeFactory.create(
          dataSource.getKsqlTopic().getValueFormat().getFormatInfo(),
          physicalSchema.valueSchema(),
          ksqlConfig,
          serviceContext.getSchemaRegistryClientFactory(),
          "",
          NoopProcessingLogContext.INSTANCE
      ).serializer();

      producer = serviceContext
          .getKafkaClientSupplier()
          .getProducer(ksqlConfig.getProducerClientConfigProps());
    }

    public void insertRow(JsonObject row) {

      Struct key = new Struct(dataSource.getSchema().keyConnectSchema());

      // TODO validate schema

      for (Field field : key.schema().fields()) {
        Object value = row.getValue(field.name());
        key.put(field, value);
      }

      List<Object> valList = new ArrayList<>(row.size());
      for (Field field : dataSource.getSchema().valueConnectSchema().fields()) {
        valList.add(row.getValue(field.name()));
      }
      GenericRow value = new GenericRow(valList);

      final byte[] keyBytes = keySerializer.serialize(topicName, key);
      final byte[] valueBytes = valueSerializer.serialize(topicName, value);

      ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
          topicName,
          (Integer) null,
          System.currentTimeMillis(),
          keyBytes,
          valueBytes
      );

      producer.send(record, this::sendCallback);
    }

    private void sendCallback(RecordMetadata metadata, Exception exception) {
      // TODO send acks back to client
    }

  }

}
