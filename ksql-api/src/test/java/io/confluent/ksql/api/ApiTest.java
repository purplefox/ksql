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
import static org.junit.Assert.assertFalse;

import io.confluent.ksql.api.ApiConnection.ChannelHandlerFactory;
import io.confluent.ksql.api.client.KsqlDBClient;
import io.confluent.ksql.api.client.KsqlDBConnection;
import io.confluent.ksql.api.client.QueryResult;
import io.confluent.ksql.api.client.Row;
import io.confluent.ksql.api.server.ApiServer;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
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
  private KsqlDBClient client;

  private volatile TestRowProvider testRowProvider;
  private volatile TestInserter testInserter;

  @Before
  public void setUp() throws Throwable {

    vertx = Vertx.vertx();

    Map<Short, ChannelHandlerFactory> channelHandlerFactories = new HashMap<>();
    channelHandlerFactories
        .put(ApiConnection.REQUEST_TYPE_QUERY,
            (channelID, conn) -> new TestQueryAction(channelID, conn, vertx, testRowProvider));
    channelHandlerFactories
        .put(ApiConnection.REQUEST_TYPE_INSERT,
            (channelID, conn) -> new TestInsertAction(channelID, conn, testInserter));

    server = new ApiServer(channelHandlerFactories, vertx);
    server.start().get();
    client = KsqlDBClient.client();
  }

  @After
  public void tearDown() {
  }

  @Test
  public void testStreamPushQuery() throws Throwable {

    JsonArray colNames = colNames("col0", "col1", "col2");
    JsonArray colTypes = colTypes("INT", "STRING", "BOOLEAN");
    List<JsonArray> rows = generateRows(10, colTypes);
    int queryID = 12345;
    testRowProvider = new TestRowProvider(colNames, colTypes, rows, queryID, false);

    TestSubscriber<Row> subscriber = new TestSubscriber<>();

    CompletableFuture<QueryResult> queryFut =
        connect()
            .thenCompose(
                con -> con.session().streamQuery("select * from line_items", false));

    QueryResult res = queryFut.get();
    assertEquals(queryID, res.queryID());

    res.subscribe(subscriber);

    List<Row> items = subscriber.waitForItems(10, 10000);
    assertEquals(10, items.size());
    assertFalse(subscriber.isCompleted());
    System.out.println(items);
  }

  @Test
  public void testPullQuery() throws Throwable {

    JsonArray colNames = colNames("col0", "col1", "col2");
    JsonArray colTypes = colTypes("INT", "STRING", "BOOLEAN");
    List<JsonArray> rows = generateRows(10, colTypes);
    int queryID = 12345;
    testRowProvider = new TestRowProvider(colNames, colTypes, rows, queryID, true);

    CompletableFuture<List<Row>> queryFut =
        connect().thenCompose(con -> con.session().executeQuery("select * from line_items"));

    List<Row> items = queryFut.get();
    assertEquals(rows.size(), items.size());

    for (int i = 0; i < items.size(); i++) {
      assertEquals(rows.get(i), items.get(i).values());
      assertEquals(colNames, items.get(i).columns());
      assertEquals(colTypes, items.get(i).columnTypes());
    }

    System.out.println(items);
  }

  @Test
  public void testInsert() throws Exception {

    testInserter = new TestInserter();

    JsonObject row = new JsonObject();

    connect().thenCompose(con -> con.session().insertInto("line_items", row));

    List<JsonObject> items = waitForItems(1, 10000, testInserter);
    assertEquals(1, items.size());
    assertEquals(row, items.get(0));

    System.out.println(items);
  }

  List<JsonObject> waitForItems(int num, long timeout, TestInserter testInserter) throws Exception {
    long start = System.currentTimeMillis();
    while ((System.currentTimeMillis() - start) < timeout) {
      synchronized (this) {
        List<JsonObject> rows = testInserter.getRows();
        if (rows.size() >= num) {
          return rows;
        }
        Thread.sleep(10);
      }
    }
    throw new TimeoutException("Timed out waiting for items");
  }

  private CompletableFuture<KsqlDBConnection> connect() {
    return client.connectWebsocket("localhost", 8888);
  }

  private List<JsonArray> generateRows(int num, JsonArray colTypes) {
    List<JsonArray> rows = new ArrayList<>();
    for (int i = 0; i < num; i++) {
      JsonArray row = new JsonArray();
      for (int j = 0; j < colTypes.size(); j++) {
        String colType = colTypes.getString(j);
        switch (colType) {
          case "STRING": {
            row.add("value" + i + "-" + j);
            break;
          }
          case "INT": {
            row.add(i + j);
            break;
          }
          case "BOOLEAN": {
            row.add((i + j) % 2 == 0);
            break;
          }
          case "DOUBLE": {
            row.add((i + j) / 3);
            break;
          }
          default:
            throw new IllegalArgumentException("Invalid type " + colType);
        }
      }
      rows.add(row);
    }
    return rows;
  }


  private JsonArray colNames(String... colNames) {
    return new JsonArray(Arrays.asList(colNames));
  }

  private JsonArray colTypes(String... colTypes) {
    return new JsonArray(Arrays.asList(colTypes));
  }

  private JsonArray row(Object... vals) {
    return new JsonArray(Arrays.asList(vals));
  }

}
