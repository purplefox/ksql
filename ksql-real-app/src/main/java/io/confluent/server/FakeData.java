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
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.BiFunction;

public class FakeData {

  private final Stream lineItems =
      new Stream("line_item",
          new JsonArray().add("item_id").add("name").add("price"),
          new JsonArray().add("INT").add("STRING").add("DECIMAL(10, 2"));

  private final Table<Integer> users =
      new Table<>("user",
          new JsonArray().add("user_id").add("first_name").add("last_name"),
          new JsonArray().add("INT").add("STRING").add("STRING"),
          this::aggregateUsers);

  private final Stream basketEvents =
      new Stream("basket_event",
          new JsonArray().add("user_id").add("item_id").add("amount"),
          new JsonArray().add("INT").add("INT").add("INT"));

  private final Table<Integer> userBaskets =
      new Table<>("user_basket",
          new JsonArray().add("user_id").add("item_id").add("amount"),
          new JsonArray().add("INT").add("INT").add("INT"),
          this::aggregateBasket);

  private final Stream orderEvents =
      new Stream("order_event",
          new JsonArray().add("user_id").add("order"),
          new JsonArray().add("INT").add("STRUCT"));

  private final Table<Integer> orderReport =
      new Table<>("order_report",
          new JsonArray().add("item_id").add("item_name").add("sold"),
          new JsonArray().add("INT").add("STRING").add("DECIMAL(10, 2)"),
          this::aggregateOrderReport);

  public FakeData() {
    setupReferenceData();
    basketEvents.addSubscriber(userBaskets);
    orderEvents.addSubscriber(orderReport);
  }

  public RowProvider getRowProvider(String queryString, boolean pull) {
    if (queryString.equals("SELECT * FROM LINE_ITEM")) {
      return new QueryRowProvider(123, lineItems, pull);
    } else if (queryString.startsWith("SELECT * FROM USER_BASKET WHERE USER_ID =")) {
      return new QueryRowProvider(345, userBaskets, pull);
    }
    throw new IllegalArgumentException("Unknown query " + queryString);
  }

  public Inserter getInserter(String containerName) {
    if (containerName.equals("basket_events")) {
      return new FakeInserter(basketEvents);
    }
    throw new IllegalArgumentException("Invalid container " + containerName);
  }

  private void setupReferenceData() {
    lineItems.addRow(new JsonArray(Arrays.asList(1, "Sausages", 2.99)));
    lineItems.addRow(new JsonArray(Arrays.asList(2, "Avocados", 1.50)));
    lineItems.addRow(new JsonArray(Arrays.asList(3, "Bicycle", 249.95)));
    lineItems.addRow(new JsonArray(Arrays.asList(4, "Gerbil", 8.75)));
    lineItems.addRow(new JsonArray(Arrays.asList(5, "Book", 9.99)));

    users.addRow(new JsonArray(Arrays.asList(1, "john")));
    users.addRow(new JsonArray(Arrays.asList(2, "jane")));
  }

  interface Container extends Subscriber {

    void addRow(JsonArray row);

    JsonArray getColNames();

    JsonArray getColTypes();

    List<JsonArray> getRows();

    void addSubscriber(Subscriber subscriber);
  }

  interface Subscriber {

    void addRow(JsonArray row);
  }

  static class Stream implements Container {

    private final String name;
    private final JsonArray colNames;
    private final JsonArray colTypes;
    private final List<JsonArray> rows = new ArrayList<>();
    private final List<Subscriber> subscribers = new ArrayList<>();

    public Stream(String name, JsonArray colNames, JsonArray colTypes) {
      this.name = name;
      this.colNames = colNames;
      this.colTypes = colTypes;
    }

    @Override
    public synchronized void addRow(JsonArray row) {
      System.out.println("Inserting " + row + " into stream " + name);
      rows.add(row);
      for (Subscriber subscriber : subscribers) {
        System.out.println("Forwarding to subscriber " + subscriber);
        subscriber.addRow(row);
      }
    }

    @Override
    public JsonArray getColNames() {
      return colNames;
    }

    @Override
    public JsonArray getColTypes() {
      return colTypes;
    }

    public synchronized void addSubscriber(Subscriber subscriber) {
      this.subscribers.add(subscriber);
    }

    public synchronized List<JsonArray> getRows() {
      return new ArrayList<>(rows);
    }
  }

  JsonArray aggregateBasket(Map<Integer, JsonArray> rows, JsonArray row) {
    Integer key = row.getInteger(0);
    JsonArray prev = rows.get(key);
    if (prev == null) {
      rows.put(key, row);
      return row;
    } else {
      int amount = prev.getInteger(1);
      row.set(1, row.getInteger(1) + amount);
      rows.put(key, row);
      return row;
    }
  }

  JsonArray aggregateUsers(Map<Integer, JsonArray> rows, JsonArray row) {
    rows.put(row.getInteger(0), row);
    return row;
  }

  JsonArray aggregateOrderReport(Map<Integer, JsonArray> rows, JsonArray row) {
    Integer key = row.getInteger(0);
    JsonArray prev = rows.get(key);
    if (prev == null) {
      rows.put(key, row);
      return row;
    } else {
      int amount = prev.getInteger(1);
      row.set(1, row.getInteger(1) + amount);
      rows.put(key, row);
      return row;
    }
  }

  static class Table<T extends Comparable<?>> implements Container {

    private final String name;
    private final JsonArray colNames;
    private final JsonArray colTypes;
    private final Map<T, JsonArray> rows = new TreeMap<>();
    private final List<Subscriber> subscribers = new ArrayList<>();

    public Table(String name, JsonArray colNames, JsonArray colTypes,
        BiFunction<Map<T, JsonArray>, JsonArray, JsonArray> aggregation) {
      this.name = name;
      this.colNames = colNames;
      this.colTypes = colTypes;
      this.aggregation = aggregation;
    }

    // I know, not a pure function!
    private final BiFunction<Map<T, JsonArray>, JsonArray, JsonArray> aggregation;

    @Override
    public void addRow(JsonArray row) {
      System.out.println("Inserting " + row + " into table " + name);
      JsonArray changed = aggregation.apply(rows, row);
      System.out.println("Changed: " + changed + " original " + row);
      if (changed != null) {
        for (Subscriber subscriber : subscribers) {
          System.out.println("Forwarding to subscriber " + subscriber);
          subscriber.addRow(changed);
        }
      }
    }

    @Override
    public JsonArray getColNames() {
      return colNames;
    }

    @Override
    public JsonArray getColTypes() {
      return colTypes;
    }

    public synchronized void addSubscriber(Subscriber queryRowProvider) {
      this.subscribers.add(queryRowProvider);
    }

    public synchronized List<JsonArray> getRows() {
      return new ArrayList<>(rows.values());
    }
  }


  class QueryRowProvider implements RowProvider, Subscriber {

    private final int queryID;
    private final Container container;
    private int pos;
    private final List<JsonArray> rows = new ArrayList<>();
    private final boolean terminal;

    public QueryRowProvider(int queryID, Container container,
        boolean terminal) {
      this.queryID = queryID;
      this.container = container;
      this.terminal = terminal;
      rows.addAll(container.getRows());
      container.addSubscriber(this);
    }

    public synchronized void addRow(JsonArray row) {
      System.out.println("Adding row to query provider");
      if (terminal) {
        return;
      }
      System.out.println("Added row");
      rows.add(row);
    }

    @Override
    public synchronized int available() {
      return rows.size() - pos;
    }

    @Override
    public synchronized Buffer poll() {
      JsonArray row = rows.get(pos);
      pos++;
      return row.toBuffer();
    }

    @Override
    public void start() {
    }

    @Override
    public synchronized boolean complete() {
      if (!terminal) {
        return false;
      }
      return pos >= rows.size();
    }

    @Override
    public JsonArray colNames() {
      return container.getColNames();
    }

    @Override
    public JsonArray colTypes() {
      return container.getColTypes();
    }

    @Override
    public int queryID() {
      return queryID;
    }
  }


  class FakeInserter implements Inserter {

    private final Stream container;

    public FakeInserter(Stream container) {
      this.container = container;
    }

    @Override
    public void insertRow(JsonObject row) {
      container.addRow(new JsonArray(new ArrayList<>(row.getMap().values())));
    }
  }


}
