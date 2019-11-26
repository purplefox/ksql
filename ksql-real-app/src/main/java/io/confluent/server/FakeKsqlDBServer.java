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

package io.confluent.server;

import io.confluent.ksql.api.ApiConnection;
import io.confluent.ksql.api.ApiConnection.MessageHandlerFactory;
import io.confluent.ksql.api.server.ApiServer;
import io.confluent.ksql.api.server.actions.InsertAction;
import io.confluent.ksql.api.server.actions.Inserter;
import io.confluent.ksql.api.server.actions.QueryAction;
import io.confluent.ksql.api.server.actions.RowProvider;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class FakeKsqlDBServer {

  private final Vertx vertx;
  private final ApiServer apiServer;

  public static void main(String[] args) throws Exception {
    FakeKsqlDBServer server = new FakeKsqlDBServer();
    server.start().whenComplete((v, t) -> {
      if (t != null) {
        t.printStackTrace();
      } else {
        System.out.println("Fake ksqlDB Server started");
      }
    });
  }

  public FakeKsqlDBServer() {
    this.vertx = Vertx.vertx();
    Map<String, MessageHandlerFactory> messageHandlerFactories = new HashMap<>();
    FakeData fakeData = new FakeData();
    messageHandlerFactories
        .put("query", (conn, msg) -> new FakeQueryAction(conn, msg, vertx, fakeData));
    messageHandlerFactories.put("insert", (conn, msg) -> new FakeInsertAction(conn, msg, fakeData));
    this.apiServer = new ApiServer(messageHandlerFactories, vertx);
  }

  public CompletableFuture<Void> start() {
    return apiServer.start();
  }

  public CompletableFuture<Void> stop() {
    return apiServer.stop();
  }

  class FakeQueryAction extends QueryAction {

    private final FakeData fakeData;

    public FakeQueryAction(ApiConnection apiConnection,
        JsonObject message, Vertx vertx, FakeData fakeData) {
      super(apiConnection, message, vertx);
      this.fakeData = fakeData;
    }

    @Override
    protected RowProvider createRowProvider(String queryString) {
      return fakeData.getRowProvider(queryString, !queryString.contains("EMIT CHANGES"));
    }
  }

  class FakeInsertAction extends InsertAction {

    private final FakeData fakeData;

    FakeInsertAction(ApiConnection apiConnection, JsonObject message, FakeData fakeData) {
      super(apiConnection, message);
      this.fakeData = fakeData;
    }

    @Override
    protected Inserter createInserter(Integer channelID, String target) {
      System.out.println("Creating inserter for " + channelID);
      return fakeData.getInserter(target);
    }

  }
}
