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

import io.confluent.ksql.api.server.actions.Inserter;
import io.confluent.ksql.api.server.actions.RowProvider;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class FakeData {

  // Table
  private final Container lineItems =
      new Container("line_item",
          new JsonArray().add("item_id").add("name").add("price"),
          new JsonArray().add("INT").add("STRING").add("DECIMAL(10, 2"));

  // Table
  private final Container users =
      new Container("user",
          new JsonArray().add("user_id").add("first_name").add("last_name"),
          new JsonArray().add("INT").add("STRING").add("STRING"));

  // Stream
  private final Container basketEvents =
      new Container("basket_event",
          new JsonArray().add("user_id").add("item_id").add("amount"),
          new JsonArray().add("INT").add("INT").add("INT"));

  // Table
  private final Container userBaskets =
      new Container("user_basket",
          new JsonArray().add("user_id").add("item_id").add("amount"),
          new JsonArray().add("INT").add("INT").add("INT"));

  // Stream
  private final Container orderEvents =
      new Container("order_event",
          new JsonArray().add("user_id").add("order"),
          new JsonArray().add("INT").add("STRUCT"));

  // Table
  private final Container orderReport =
      new Container("order_report",
          new JsonArray().add("item_id").add("item_name").add("sold"),
          new JsonArray().add("INT").add("STRING").add("DECIMAL(10, 2)"));

  public FakeData() {
    setupReferenceData();
  }

  public RowProvider getRowProvider(String queryString, boolean pull) {
    if (queryString.equals("SELECT * FROM LINE_ITEMS")) {
      return new FakeRowProvider(lineItems, 0, pull);
    } else if (queryString.startsWith("SELECT * FROM USER_BASKET WHERE USER_ID =")) {
      return new FakeRowProvider(userBaskets, 0, pull);
    }
    throw new IllegalArgumentException("Unknown query " + queryString);
  }

  public Inserter getInserter(String containerName) {
    if (containerName.equals("BASKET_EVENT")) {
      return new FakeInserter(basketEvents);
    }
    throw new IllegalArgumentException("Invalid container " + containerName);
  }

  private void setupReferenceData() {
    lineItems.rows.add(new JsonArray(Arrays.asList(1, "Sausages", 2.99)));
    lineItems.rows.add(new JsonArray(Arrays.asList(2, "Avocados", 1.50)));
    lineItems.rows.add(new JsonArray(Arrays.asList(3, "Bicycle", 249.95)));
    lineItems.rows.add(new JsonArray(Arrays.asList(4, "Gerbil", 8.75)));
    lineItems.rows.add(new JsonArray(Arrays.asList(5, "Book", 9.99)));

    users.rows.add(new JsonArray(Arrays.asList(1, "john")));
    users.rows.add(new JsonArray(Arrays.asList(2, "jane")));
  }

  static class Container {

    private final String name;
    private final JsonArray colNames;
    private final JsonArray colTypes;
    private final List<JsonArray> rows = new ArrayList<>();

    public Container(String name, JsonArray colNames, JsonArray colTypes) {
      this.name = name;
      this.colNames = colNames;
      this.colTypes = colTypes;
    }
  }

  class FakeRowProvider implements RowProvider {

    private final Container container;
    private final int queryID;
    private final boolean terminal;

    private Iterator<JsonArray> iter;
    private int pos;

    public FakeRowProvider(Container container, int queryID, boolean terminal) {
      this.container = container;
      this.queryID = queryID;
      this.terminal = terminal;
      this.iter = container.rows.iterator();
    }

    @Override
    public int available() {
      return container.rows.size() - pos;
    }

    @Override
    public synchronized Buffer poll() {
      pos++;
      JsonArray arr = iter.next();
      return arr.toBuffer();
    }

    @Override
    public void start() {
    }

    @Override
    public synchronized boolean complete() {
      if (!terminal) {
        return false;
      }
      return pos >= container.rows.size();
    }

    @Override
    public JsonArray colNames() {
      return container.colNames;
    }

    @Override
    public JsonArray colTypes() {
      return container.colTypes;
    }

    @Override
    public int queryID() {
      return queryID;
    }
  }

  class FakeInserter implements Inserter {

    private final Container container;

    public FakeInserter(Container container) {
      this.container = container;
    }

    @Override
    public void insertRow(JsonObject row) {
      container.rows.add(new JsonArray(new ArrayList<>(row.getMap().values())));
    }
  }


}
