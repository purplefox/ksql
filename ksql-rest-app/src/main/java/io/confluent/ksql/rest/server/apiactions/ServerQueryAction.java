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
import io.confluent.ksql.api.server.actions.QueryAction;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.rest.entity.TableRowsEntity;
import io.confluent.ksql.rest.server.execution.StaticQueryExecutor;
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
import io.vertx.core.json.JsonObject;
import java.security.Principal;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import org.apache.kafka.streams.KeyValue;

public class ServerQueryAction extends QueryAction {

  private final KsqlEngine ksqlEngine;
  private final KsqlConfig ksqlConfig;
  private final KsqlSecurityExtension securityExtension;
  private final UserServiceContextFactory serviceContextFactory;
  private final DefaultServiceContextFactory defaultServiceContextFactory;

  public ServerQueryAction(ApiConnection apiConnection,
      JsonObject message, Vertx vertx, KsqlEngine ksqlEngine, KsqlConfig ksqlConfig,
      KsqlSecurityExtension ksqlSecurityExtension) {
    super(apiConnection, message, vertx);
    this.ksqlEngine = ksqlEngine;
    this.ksqlConfig = ksqlConfig;
    this.securityExtension = ksqlSecurityExtension;
    this.serviceContextFactory = RestServiceContextFactory::create;
    this.defaultServiceContextFactory = RestServiceContextFactory::create;
  }

  @Override
  protected RowProvider createRowProvider(String queryString) {
    Principal principal = new DummyPrincipal();
    ConfiguredStatement<Query> configured = createStatement(queryString);
    ServiceContext serviceContext = createServiceContext(principal);
    if (configured.getStatement().isStatic()) {
      return createStaticQueryRowProvider(serviceContext, configured);
    } else {
      return createNonStaticQueryRowProvider(serviceContext, configured);
    }
  }

  private RowProvider createStaticQueryRowProvider(ServiceContext serviceContext,
      ConfiguredStatement<Query> configured) {
    TableRowsEntity result = StaticQueryExecutor.execute(configured, ksqlEngine, serviceContext);
    return new PullQueryRowProvider(result);
  }

  private RowProvider createNonStaticQueryRowProvider(ServiceContext serviceContext,
      ConfiguredStatement<Query> configured) {
    TransientQueryMetadata queryMetadata =
        (TransientQueryMetadata) ksqlEngine.execute(serviceContext, configured)
            .getQuery()
            .get();
    return new PushQueryRowProvider(queryMetadata);
  }

  private ConfiguredStatement<Query> createStatement(String queryString) {
    final List<ParsedStatement> statements = ksqlEngine.parse(queryString);
    if ((statements.size() != 1)) {
      handleError(
          String
              .format("Expected exactly one KSQL statement; found %d instead", statements.size()));
    }
    PreparedStatement<?> ps = ksqlEngine.prepare(statements.get(0));
    final Statement statement = ps.getStatement();
    if (!(statement instanceof Query)) {
      handleError("Invalid query: " + queryString);
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

  private static class PushQueryRowProvider implements RowProvider {

    private final TransientQueryMetadata queryMetadata;
    private final BlockingQueue<KeyValue<String, GenericRow>> queue;

    PushQueryRowProvider(TransientQueryMetadata queryMetadata) {
      this.queryMetadata = queryMetadata;
      this.queue = queryMetadata.getRowQueue();
    }

    @Override
    public int available() {
      return queue.size();
    }

    @Override
    public Buffer poll() {
      KeyValue<String, GenericRow> kv = queue.poll();
      if (kv == null) {
        throw new IllegalStateException("No row to poll");
      }
      GenericRow row = kv.value;
      return Json.encodeToBuffer(row.getColumns());
    }

    @Override
    public void start() {
      queryMetadata.start();
    }

    @Override
    public boolean complete() {
      return false;
    }
  }

  private static class PullQueryRowProvider implements RowProvider {

    private final TableRowsEntity results;
    private final List<List<?>> rows;
    private final Iterator<List<?>> iter;
    private int pos;

    public PullQueryRowProvider(TableRowsEntity results) {
      this.results = results;
      this.rows = results.getRows();
      iter = rows.iterator();
    }

    @Override
    public int available() {
      return rows.size() - pos;
    }

    @Override
    public Buffer poll() {
      List<?> row = iter.next();
      Buffer buff = Json.encodeToBuffer(row);
      pos++;
      return buff;
    }

    @Override
    public void start() {
    }

    @Override
    public boolean complete() {
      return available() == 0;
    }
  }

  private static class DummyPrincipal implements Principal {

    @Override
    public String getName() {
      return "tim";
    }
  }

}
