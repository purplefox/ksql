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

package io.confluent.ksql.api;

import static org.junit.Assert.assertEquals;

import io.confluent.ksql.api.ApiConnection.MessageHandlerFactory;
import io.confluent.ksql.api.client.KSqlClient;
import io.confluent.ksql.api.client.Row;
import io.confluent.ksql.api.server.Server;
import io.confluent.ksql.rest.server.resources.streaming.Flow.Subscriber;
import io.confluent.ksql.rest.server.resources.streaming.Flow.Subscription;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ApiTest {

  private Server server;
  private KSqlClient client;

  @Before
  public void setUp() throws Throwable {

    Map<String, MessageHandlerFactory> messageHandlerFactories = new HashMap<>();
    messageHandlerFactories.put("query", TestQueryMessageHandler::new);

    server = new Server(messageHandlerFactories);
    server.start().get();
    client = KSqlClient.client();
  }

  @After
  public void tearDown() {
  }

  @Test
  public void testStreamPushQuery() throws Throwable {

    TestSubscriber<Row> subscriber = new TestSubscriber<>();

    CompletableFuture<Integer> queryFut =
        client.connectWebsocket("localhost", 8888)
            .thenCompose(con -> con.streamQuery("select * from line_items", false, subscriber));

    Integer res = queryFut.get();
    assertEquals(0, res.intValue());

    List<Row> items = subscriber.waitForItems(10, 10000);
    assertEquals(10, items.size());
    System.out.println(items);
  }

  @Test
  public void testExecuteQuery() throws Throwable {

    CompletableFuture<List<Row>> queryFut =
        client.connectWebsocket("localhost", 8888)
            .thenCompose(con -> con.executeQuery("select * from line_items"));

    List<Row> items = queryFut.get();
    assertEquals(10, items.size());
    System.out.println(items);
  }

  static class TestQueryMessageHandler implements Runnable {

    private final ApiConnection apiConnection;
    private final JsonObject message;

    TestQueryMessageHandler(ApiConnection apiConnection, JsonObject message) {
      this.apiConnection = apiConnection;
      this.message = message;
    }

    public void run() {
      int queryID = 0;

      Integer channelID = message.getInteger("channel-id");

      JsonArray cols = new JsonArray();
      JsonArray colTypes = new JsonArray();
      for (int i = 0; i < 10; i++) {
        cols.add("col" + i);
        colTypes.add("STRING");
      }

      JsonObject response = new JsonObject()
          .put("type", "reply")
          .put("request-id", message.getInteger("request-id"))
          .put("query-id", queryID)
          .put("status", "ok")
          .put("cols", cols)
          .put("col-types", colTypes);

      apiConnection.writeMessage(response);

      for (int i = 0; i < 10; i++) {
        JsonArray data = jsonArray(i);
        apiConnection.protocolHandler.writeDataFrame(channelID, data.toBuffer());
      }

      boolean pull = message.getBoolean("pull");
      if (pull) {
        apiConnection.protocolHandler.writeCloseFrame(channelID);
      }
    }
  }

  private static JsonArray jsonArray(int n) {
    JsonArray arr = new JsonArray();
    for (int i = 0; i < 10; i++) {
      arr.add(n + "-value-" + i);
    }
    return arr;
  }

  static class TestSubscriber<T> implements Subscriber<T> {

    private final List<T> items = new ArrayList<>();
    private Throwable error;
    private boolean completed;
    private Subscription subscription;

    @Override
    public synchronized void onNext(T item) {
      if (subscription == null) {
        throw new IllegalStateException("subscription has not been set");
      }
      subscription.request(1);
      items.add(item);
    }

    @Override
    public synchronized void onError(Throwable e) {
      error = e;
    }

    @Override
    public synchronized void onComplete() {
      completed = true;
    }

    @Override
    public void onSchema(LogicalSchema schema) {
    }

    @Override
    public synchronized void onSubscribe(Subscription subscription) {
      this.subscription = subscription;
    }

    public List<T> waitForItems(int num, long timeout) throws Exception {
      long start = System.currentTimeMillis();
      while ((System.currentTimeMillis() - start) < timeout) {
        synchronized (this) {
          if (items.size() >= num) {
            return items;
          }
          Thread.sleep(10);
        }
      }
      throw new TimeoutException("Timed out waiting for items");
    }
  }

}
