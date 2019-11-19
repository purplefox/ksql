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

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.api.ApiConnection;
import io.confluent.ksql.api.protocol.ChannelHandler;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.rest.server.services.RestServiceContextFactory;
import io.confluent.ksql.rest.server.services.RestServiceContextFactory.DefaultServiceContextFactory;
import io.confluent.ksql.rest.server.services.RestServiceContextFactory.UserServiceContextFactory;
import io.confluent.ksql.security.KsqlSecurityExtension;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.TransientQueryMetadata;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.security.Principal;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.streams.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryAction implements ChannelHandler, Runnable {

  private static final Logger log = LoggerFactory.getLogger(QueryAction.class);

  private final ApiConnection apiConnection;
  private final JsonObject message;
  private final KsqlEngine ksqlEngine;
  private final KsqlConfig ksqlConfig;
  private final KsqlSecurityExtension securityExtension;
  private final UserServiceContextFactory serviceContextFactory;
  private final DefaultServiceContextFactory defaultServiceContextFactory;
  private TransientQueryMetadata queryMetadata;
  private int channelID;
  private int bytes;
  private Buffer holding;
  private final Vertx vertx;

  public QueryAction(ApiConnection apiConnection, JsonObject message,
      KsqlEngine ksqlEngine, KsqlConfig ksqlConfig,
      KsqlSecurityExtension securityExtension,
      Vertx vertx) {
    this.apiConnection = apiConnection;
    this.message = message;
    this.ksqlEngine = ksqlEngine;
    this.ksqlConfig = ksqlConfig;
    this.securityExtension = securityExtension;
    this.defaultServiceContextFactory = RestServiceContextFactory::create;
    this.serviceContextFactory = RestServiceContextFactory::create;
    this.vertx = vertx;
  }

  @Override
  public synchronized void run() {

    Integer channelID = message.getInteger("channel-id");
    if (channelID == null) {
      apiConnection.handleError("Message must contain a channel-id field");
      return;
    }
    this.channelID = channelID;
    String queryString = message.getString("query");
    if (queryString == null) {
      apiConnection.handleError("Control message must contain a query field");
    }

    Principal principal = new DummyPrincipal();

    ConfiguredStatement<Query> configured = createStatement(queryString);
    ServiceContext serviceContext = createServiceContext(principal);

    queryMetadata =
        (TransientQueryMetadata) ksqlEngine.execute(serviceContext, configured)
            .getQuery()
            .get();

    int queryID = 123;
    JsonArray cols = new JsonArray().add("a").add("b");
    JsonArray colTypes = new JsonArray().add("STRING").add("STRING");

    JsonObject response = new JsonObject()
        .put("type", "reply")
        .put("request-id", message.getInteger("request-id"))
        .put("query-id", queryID)
        .put("status", "ok")
        .put("cols", cols)
        .put("col-types", colTypes);

    apiConnection.writeMessage(response);

    this.bytes = 1024 * 1024; // Initial window size;

    /*
    TODO this is a hack!
    Query messages are currently put on a blocking queue
    Ideally Kafka Streams would support back pressure and would directly publish to us
    (like a reactive streams publisher), but that's not going to happen easily.
    Instead of using a blocking queue - the KS foreach should add directly onto the QueryAction and
    block when there are no window bytes available
    But for now... we poll. I'm sorry, I'm really, really sorry :((
    */
    setDeliverTimer();

    queryMetadata.start();
  }

  private void setDeliverTimer() {
    vertx.setTimer(100, h -> {
      checkDeliver();
      setDeliverTimer();
    });
  }

  static class DummyPrincipal implements Principal {

    @Override
    public String getName() {
      return "tim";
    }
  }

  private ConfiguredStatement<Query> createStatement(String queryString) {
    final List<ParsedStatement> statements = ksqlEngine.parse(queryString);
    if ((statements.size() != 1)) {
      apiConnection.handleError(
          String
              .format("Expected exactly one KSQL statement; found %d instead", statements.size()));
    }
    PreparedStatement<?> ps = ksqlEngine.prepare(statements.get(0));
    final Statement statement = ps.getStatement();
    if (!(statement instanceof Query)) {
      apiConnection.handleError("Invalid query: " + queryString);
    }
    @SuppressWarnings("unchecked")
    PreparedStatement<Query> psq = (PreparedStatement<Query>) ps;
    final Map<String, Object> clientLocalProperties = ImmutableMap
        .of("ksql.streams.auto.offset.reset", "earliest");
    final ConfiguredStatement<Query> configured =
        ConfiguredStatement.of(psq, clientLocalProperties, ksqlConfig);
    return configured;
  }

  private ServiceContext createServiceContext(final Principal principal) {
    // Creates a ServiceContext using the user's credentials, so the WS query topics are
    // accessed with the user permission context (defaults to KSQL service context)

    if (!securityExtension.getUserContextProvider().isPresent()) {
      return defaultServiceContextFactory.create(ksqlConfig, Optional.empty());
    }

    return securityExtension.getUserContextProvider()
        .map(provider ->
            serviceContextFactory.create(
                ksqlConfig,
                Optional.empty(),
                provider.getKafkaClientSupplier(principal),
                provider.getSchemaRegistryClientFactory(principal)
            ))
        .get();
  }

  @Override
  public void handleData(Buffer data) {
  }

  @Override
  public synchronized void handleFlow(int bytes) {
    this.bytes += bytes;
    checkDeliver();
  }

  private synchronized void checkDeliver() {
    if (bytes == 0) {
      return;
    }
    if (holding != null) {
      if (this.bytes >= holding.length()) {
        sendBuffer(holding);
        holding = null;
      } else {
        return;
      }
    }
    int num = queryMetadata.getRowQueue().size();
    for (int i = 0; i < num; i++) {
      KeyValue<String, GenericRow> kv = queryMetadata.getRowQueue().poll();
      GenericRow row = kv.value;
      Buffer buff = Json.encodeToBuffer(row.getColumns());
      if (bytes >= buff.length()) {
        sendBuffer(buff);
      } else {
        holding = buff;
        break;
      }
    }
  }

  private void sendBuffer(Buffer buffer) {
    apiConnection.writeDataFrame(channelID, buffer);
    bytes -= buffer.length();
  }

  @Override
  public void handleClose() {

  }

}
