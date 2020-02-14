/*
 * Copyright 2020 Confluent Inc.
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

import static io.confluent.ksql.api.utils.TestUtils.findFilePath;
import static io.confluent.ksql.test.util.AssertEventually.assertThatEventually;
import static org.hamcrest.Matchers.hasSize;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ClientOptions;
import io.confluent.ksql.api.client.QueryResult;
import io.confluent.ksql.api.client.Row;
import io.confluent.ksql.api.client.impl.ClientOptionsImpl;
import io.confluent.ksql.api.server.ApiServerConfig;
import io.confluent.ksql.api.server.Server;
import io.confluent.ksql.api.utils.ListRowGenerator;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApiTest {

  private static final Logger log = LoggerFactory.getLogger(ApiTest.class);

  private static final JsonArray DEFAULT_COLUMN_NAMES = new JsonArray().add("name").add("age")
      .add("male");
  private static final JsonArray DEFAULT_COLUMN_TYPES = new JsonArray().add("STRING").add("INT")
      .add("BOOLEAN");
  private static final List<JsonArray> DEFAULT_ROWS = generateRows();
  private static final JsonObject DEFAULT_PUSH_QUERY_REQUEST_PROPERTIES = new JsonObject()
      .put("prop1", "val1").put("prop2", 23);
  private static final String DEFAULT_PULL_QUERY = "select * from foo where rowkey='1234';";
  private static final String DEFAULT_PUSH_QUERY = "select * from foo emit changes;";

  private Vertx vertx;
  private Server server;
  private TestEndpoints testEndpoints;

  private Client client;

  @Before
  public void setUp() {

    vertx = Vertx.vertx();
    vertx.exceptionHandler(t -> log.error("Unhandled exception in Vert.x", t));

    JsonObject config = new JsonObject()
        .put("ksql.apiserver.listen.host", "localhost")
        .put("ksql.apiserver.listen.port", 8089)
        .put("ksql.apiserver.key.path", findFilePath("test-server-key.pem"))
        .put("ksql.apiserver.cert.path", findFilePath("test-server-cert.pem"))
        .put("ksql.apiserver.verticle.instances", 4);

    testEndpoints = new TestEndpoints();
    server = new Server(vertx, new ApiServerConfig(config), testEndpoints);
    server.start();
    client = createClient();
    setDefaultRowGenerator();
  }

  @After
  public void tearDown() {
    if (client != null) {
      client.close();
    }
    if (server != null) {
      server.stop();
    }
  }

  @Test
  public void shouldExecuteQuery() throws Exception {

    QueryResult queryResult = client.streamQuery("select * from foo", new JsonObject()).get();

    TestSubscriber subscriber = new TestSubscriber();

    queryResult.subscribe(subscriber);

    assertThatEventually(subscriber::getRows, hasSize(DEFAULT_ROWS.size()));
  }


  private Client createClient() {
    ClientOptions clientOptions = new ClientOptionsImpl();
    return Client.create(clientOptions);
  }


  @SuppressWarnings("unchecked")
  private void setDefaultRowGenerator() {
    List<GenericRow> rows = new ArrayList<>();
    for (JsonArray ja : DEFAULT_ROWS) {
      rows.add(GenericRow.fromList(ja.getList()));
    }
    testEndpoints.setRowGeneratorFactory(
        () -> new ListRowGenerator(
            DEFAULT_COLUMN_NAMES.getList(),
            DEFAULT_COLUMN_TYPES.getList(),
            rows));
  }

  private static List<JsonArray> generateRows() {
    List<JsonArray> rows = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      JsonArray row = new JsonArray().add("foo" + i).add(i).add(i % 2 == 0);
      rows.add(row);
    }
    return rows;
  }

  private static List<JsonObject> generateInsertRows() {
    List<JsonObject> rows = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      JsonObject row = new JsonObject()
          .put("name", "foo" + i)
          .put("age", i)
          .put("male", i % 2 == 0);
      rows.add(row);
    }
    return rows;
  }

  private static class TestSubscriber implements Subscriber<Row> {

    private Subscription subscription;
    private final List<Row> rows = new ArrayList<>();
    private boolean completed;
    private Throwable exception;

    @Override
    public synchronized void onSubscribe(final Subscription s) {
      this.subscription = s;
      s.request(1);
    }

    @Override
    public synchronized void onNext(final Row row) {
      rows.add(row);
      subscription.request(1);
    }

    @Override
    public synchronized void onError(final Throwable t) {
      exception = t;
    }

    @Override
    public synchronized void onComplete() {
      completed = true;
    }

    public synchronized List<Row> getRows() {
      return new ArrayList<>(rows);
    }

    public synchronized boolean isCompleted() {
      return completed;
    }

    public synchronized Throwable getException() {
      return exception;
    }
  }

}
