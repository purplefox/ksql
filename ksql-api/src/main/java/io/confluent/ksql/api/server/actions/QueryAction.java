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

package io.confluent.ksql.api.server.actions;

import io.confluent.ksql.api.ApiConnection;
import io.confluent.ksql.api.protocol.ChannelHandler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public abstract class QueryAction implements ChannelHandler, Runnable {

  private final ApiConnection apiConnection;
  private final JsonObject message;
  private final Vertx vertx;
  private int channelID;
  private int bytes;
  private Buffer holding;
  private RowProvider rowProvider;
  private boolean closed;

  public QueryAction(ApiConnection apiConnection, JsonObject message, Vertx vertx) {
    this.apiConnection = apiConnection;
    this.message = message;
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

    this.rowProvider = createRowProvider(queryString);

    this.bytes = 1024 * 1024; // Initial window size;

    JsonObject response = new JsonObject()
        .put("type", "reply")
        .put("request-id", message.getInteger("request-id"))
        .put("query-id", rowProvider.queryID())
        .put("status", "ok")
        .put("cols", rowProvider.colNames())
        .put("col-types", rowProvider.colTypes());

    apiConnection.writeMessage(response);

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

    rowProvider.start();
  }

  @Override
  public void handleData(Buffer data) {
  }

  @Override
  public synchronized void handleFlow(int bytes) {
    this.bytes += bytes;
    checkDeliver();
  }

  @Override
  public void handleClose() {
    close();
  }

  protected abstract RowProvider createRowProvider(String queryString);

  protected void handleError(String errMsg) {
    apiConnection.handleError(errMsg);
  }

  private synchronized void setDeliverTimer() {
    if (closed) {
      return;
    }
    vertx.setTimer(100, h -> {
      checkDeliver();
      setDeliverTimer();
    });
  }

  private synchronized void checkDeliver() {
    if (closed) {
      return;
    }
    doCheck();
    checkComplete();
  }

  private synchronized void doCheck() {
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
    int num = rowProvider.available();
    for (int i = 0; i < num; i++) {
      Buffer buff = rowProvider.poll();
      if (bytes >= buff.length()) {
        sendBuffer(buff);
      } else {
        holding = buff;
        break;
      }
    }
  }

  private void checkComplete() {
    if (rowProvider.complete()) {
      apiConnection.writeCloseFrame(channelID);
      close();
    }
  }

  private synchronized void close() {
    closed = true;
  }

  private void sendBuffer(Buffer buffer) {
    apiConnection.writeDataFrame(channelID, buffer);
    bytes -= buffer.length();
  }

  public interface RowProvider {

    int available();

    Buffer poll();

    void start();

    boolean complete();

    JsonArray colNames();

    JsonArray colTypes();

    int queryID();
  }

}
