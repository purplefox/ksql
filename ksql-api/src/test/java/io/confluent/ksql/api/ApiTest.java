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
import io.confluent.ksql.api.flow.Subscriber;
import io.confluent.ksql.api.flow.Subscription;
import io.confluent.ksql.api.server.ApiServer;
import io.confluent.ksql.api.server.actions.QueryAction;
import io.confluent.ksql.api.server.actions.QueryAction.RowProvider;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ApiTest {

  private Vertx vertx;
  private ApiServer server;
  private KSqlClient client;

  @Before
  public void setUp() throws Throwable {

    vertx = Vertx.vertx();

    Map<String, MessageHandlerFactory> messageHandlerFactories = new HashMap<>();
    messageHandlerFactories.put("query", (conn, msg) -> new TestQueryAction(conn, msg, vertx));

    server = new ApiServer(messageHandlerFactories, vertx);
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
  public void testPullQuery() throws Throwable {

    CompletableFuture<List<Row>> queryFut =
        client.connectWebsocket("localhost", 8888)
            .thenCompose(con -> con.executeQuery("select * from line_items"));

    List<Row> items = queryFut.get();
    assertEquals(10, items.size());
    System.out.println(items);
  }

  static class TestQueryAction extends QueryAction {

    public TestQueryAction(ApiConnection apiConnection, JsonObject message,
        Vertx vertx) {
      super(apiConnection, message, vertx);
    }

    @Override
    protected RowProvider createRowProvider(String queryString) {
      return null;
    }
  }

  static class TestRowProvider implements RowProvider {

    private final JsonArray colNames;
    private final JsonArray colTypes;
    private final List<JsonArray> rows;
    private final int queryID;
    private Iterator<JsonArray> iter;
    private int pos;

    public TestRowProvider(JsonArray colNames, JsonArray colTypes,
        List<JsonArray> rows, int queryID) {
      this.colNames = colNames;
      this.colTypes = colTypes;
      this.rows = rows;
      this.queryID = queryID;
      this.iter = rows.iterator();
    }

    @Override
    public int available() {
      return rows.size() - pos;
    }

    @Override
    public Buffer poll() {
      pos++;
      JsonArray arr = iter.next();
      return arr.toBuffer();
    }

    @Override
    public void start() {
    }

    @Override
    public boolean complete() {
      return pos >= rows.size();
    }

    @Override
    public JsonArray colNames() {
      return colNames;
    }

    @Override
    public JsonArray colTypes() {
      return colTypes;
    }

    @Override
    public int queryID() {
      return queryID;
    }
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
