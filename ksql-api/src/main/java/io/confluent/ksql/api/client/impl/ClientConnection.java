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

package io.confluent.ksql.api.client.impl;

import io.confluent.ksql.api.ApiConnection;
import io.confluent.ksql.api.client.KsqlDBClientException;
import io.confluent.ksql.api.client.KsqlDBConnection;
import io.confluent.ksql.api.client.KsqlDBSession;
import io.confluent.ksql.api.client.QueryResult;
import io.confluent.ksql.api.client.Row;
import io.confluent.ksql.api.flow.Subscriber;
import io.confluent.ksql.api.flow.Subscription;
import io.confluent.ksql.api.protocol.ChannelHandler;
import io.confluent.ksql.api.protocol.ProtocolHandler.MessageFrame;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public class ClientConnection extends ApiConnection implements KsqlDBConnection {

  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling
  private final Map<Integer, Consumer<JsonObject>> requestMap = new ConcurrentHashMap<>();
  private final AtomicInteger channelIDSequence = new AtomicInteger();
  private final AtomicInteger requestIDSequence = new AtomicInteger();
  private final Map<String, InsertChannelHandler> insertChannels = new ConcurrentHashMap<>();

  public ClientConnection(Handler<Buffer> frameWriter) {
    super(frameWriter);
  }

  @Override
  public KsqlDBSession session() {
    return new Session();
  }

  @Override
  public void close() {
  }


  class Session implements KsqlDBSession {

    @Override
    public CompletableFuture<QueryResult> streamQuery(String query, boolean pull) {
      int channelID = channelIDSequence.getAndIncrement();
      int requestID = requestIDSequence.getAndIncrement();

      JsonObject message = new JsonObject()
          .put("type", "query")
          .put("query", query)
          .put("channel-id", channelID)
          .put("request-id", requestID)
          .put("pull", pull);
      CompletableFuture<QueryResult> future = new CompletableFuture<>();
      requestMap
          .put(requestID, jo -> handleQueryReply(future, jo, channelID));
      writeMessage(message);
      return future;
    }

    @Override
    public CompletableFuture<List<Row>> executeQuery(String query) {
      CompletableFuture<List<Row>> futRes = new CompletableFuture<>();
      CompletableFuture<QueryResult> fut = streamQuery(query, true);
      fut.whenComplete((qr, t) -> {
        if (t != null) {
          futRes.completeExceptionally(t);
        } else {
          qr.subscribe(new GatheringSubscriber(futRes));
        }
      });
      return futRes;
    }

    @Override
    public synchronized CompletableFuture<Void> insertInto(String target, JsonObject row) {
      CompletableFuture<Void> future = new CompletableFuture<>();
      InsertChannelHandler handler = insertChannels.get(target);
      if (handler == null) {
        int channelID = channelIDSequence.getAndIncrement();
        int requestID = requestIDSequence.getAndIncrement();
        JsonObject message = new JsonObject()
            .put("type", "insert")
            .put("target", target)
            .put("channel-id", channelID)
            .put("request-id", requestID);
        requestMap.put(requestID, jo -> handleInsertReply(future, jo));
        handler = new InsertChannelHandler(future, channelID);
        registerChannelHandler(channelID, handler);
        insertChannels.put(target, handler);
        writeMessage(message);
      }
      handler.sendInsertData(row);
      return future;
    }

  }

  @Override
  protected void runMessageHandler(Runnable messageHandler) {
    messageHandler.run();
  }

  @Override
  public void handleMessageFrame(MessageFrame messageFrame) {
    JsonObject message = messageFrame.payload;
    System.out.println("Received message on client: " + message);
    String type = message.getString("type");
    if ("reply".equals(type)) {
      Integer requestID = message.getInteger("request-id");
      if (requestID == null) {
        throw new IllegalStateException("No request-id in reply");
      }
      handleReply(requestID, message);
    } else {
      throw new IllegalStateException("Unknown type from server " + type);
    }
  }

  private void handleQueryReply(CompletableFuture<QueryResult> future, JsonObject reply,
      int channelID) {
    if (checkStatus(future, reply)) {
      Integer queryID = reply.getInteger("query-id");
      if (queryID == null) {
        future.completeExceptionally(new KsqlDBClientException("No query-id in reply"));
        return;
      }
      JsonArray columns = reply.getJsonArray("cols");
      if (columns == null) {
        throw new IllegalStateException("No cols in query reply");
      }
      JsonArray colTypes = reply.getJsonArray("col-types");
      if (colTypes == null) {
        throw new IllegalStateException("No col-types in query reply");
      }

      QueryChannelHandler handler = new QueryChannelHandler(channelID, queryID,
          new QueryResultHeader(columns, colTypes));
      registerChannelHandler(channelID, handler);

      future.complete(handler);
    }
  }

  private void handleInsertReply(CompletableFuture<Void> future, JsonObject reply) {
    checkStatus(future, reply);
  }

  class InsertChannelHandler implements ChannelHandler {

    private final CompletableFuture<Void> future;
    private final int channelID;
    private int bytes = 1024 * 1024; // Initial window size

    private final Queue<Buffer> outgoingInserts = new ConcurrentLinkedQueue<>();

    InsertChannelHandler(CompletableFuture<Void> future, int channelID) {
      this.future = future;
      this.channelID = channelID;
    }

    void sendInsertData(JsonObject row) {
      Buffer data = row.toBuffer();
      if (bytes >= data.length()) {
        bytes -= data.length();
        writeDataFrame(channelID, data);
      } else {
        outgoingInserts.add(data);
      }
    }

    private void checkSendOutgoing() {
      while (!outgoingInserts.isEmpty()) {
        Buffer data = outgoingInserts.peek();
        if (bytes >= data.length()) {
          outgoingInserts.remove();
          sendBuffer(data);
        } else {
          break;
        }
      }
    }

    private void sendBuffer(Buffer data) {
      bytes -= data.length();
      writeDataFrame(channelID, data);
    }

    @Override
    public void handleData(Buffer data) {
    }

    @Override
    public void handleAck() {

    }

    @Override
    public synchronized void handleFlow(int bytes) {
      this.bytes += bytes;
      checkSendOutgoing();
    }

    @Override
    public void handleClose() {

    }


    @Override
    public void run() {
    }
  }

  private boolean checkStatus(CompletableFuture<?> future, JsonObject reply) {
    String status = reply.getString("status");
    if (status == null) {
      throw new IllegalStateException("No status in reply");
    }
    if ("ok".equals(status)) {
      return true;
    } else if ("err".equals(status)) {
      String errMessage = reply.getString("err-msg");
      if (errMessage == null) {
        throw new IllegalStateException("No err-msg in err reply");
      }
      future.completeExceptionally(new KsqlDBClientException(errMessage));
      return false;
    } else {
      throw new IllegalStateException("Invalid status " + status);
    }
  }

  private void handleReply(int requestID, JsonObject reply) {
    if (requestMap.isEmpty()) {
      throw new IllegalStateException("No requests in map");
    }
    Consumer<JsonObject> replyHandler = requestMap.get(requestID);
    if (replyHandler == null) {
      throw new IllegalStateException("Unknown request " + requestID);
    }
    replyHandler.accept(reply);
  }

  private static class GatheringSubscriber implements Subscriber<Row> {

    private final CompletableFuture<List<Row>> futRes;
    private final List<Row> rows = new ArrayList<>();
    private Subscription subscription;

    GatheringSubscriber(CompletableFuture<List<Row>> futRes) {
      this.futRes = futRes;
    }

    @Override
    public synchronized void onNext(Row item) {
      rows.add(item);
      subscription.request(1);
    }

    @Override
    public void onError(Throwable e) {
      futRes.completeExceptionally(e);
    }

    @Override
    public synchronized void onComplete() {
      futRes.complete(rows);
    }

    @Override
    public synchronized void onSubscribe(Subscription subscription) {
      this.subscription = subscription;
      subscription.request(1);
    }
  }

  class QueryChannelHandler implements ChannelHandler, QueryResult {

    private final int channelID;
    private final int queryID;
    private final Queue<Buffer> incomingData = new ConcurrentLinkedQueue<>();
    private final QueryResultHeader header;
    private long tokenDemand;
    private boolean closed;
    private Subscriber<Row> subscriber;

    QueryChannelHandler(int channelID, int queryID,
        QueryResultHeader queryResultHeader) {
      this.channelID = channelID;
      this.queryID = queryID;
      this.header = queryResultHeader;
    }

    @Override
    public synchronized void handleData(Buffer buffer) {
      System.out.println("Received data frame on client " + buffer);
      if (closed) {
        return;
      }
      if (tokenDemand > 0) {
        System.out.println("Delivering data to subscriber");
        deliverBuffer(buffer);
      } else {
        System.out.println("Adding to incoming buffer");
        incomingData.add(buffer);
      }
    }

    @Override
    public void handleAck() {
    }

    synchronized void deliverBuffer(Buffer buffer) {
      JsonArray jsonArray = new JsonArray(buffer);
      Row row = new RowImpl(jsonArray, header);
      tokenDemand--;
      subscriber.onNext(row);
    }

    synchronized void request(long n) {
      System.out.println("Request called");
      tokenDemand += n;
      long tokens = tokenDemand; // Take copy to prevent infinite loop
      while (!incomingData.isEmpty() && tokens > 0) {
        Buffer buffer = incomingData.remove();
        ClientConnection.this.writeFlowFrame(channelID, buffer.length());
        deliverBuffer(buffer);
        tokens--;
      }
    }

    synchronized void close() {
      closed = true;
      // TODO handle the close properly
    }

    @Override
    public void handleFlow(int bytes) {
    }

    @Override
    public void handleClose() {
      System.out.println("Received close frame on client");
      subscriber.onComplete();
    }

    @Override
    public void run() {
    }

    @Override
    public int queryID() {
      return queryID;
    }

    @Override
    public List<Row> poll(int num) {
      return null;
    }

    @Override
    public Row poll() {
      return null;
    }

    @Override
    public synchronized void subscribe(Subscriber<Row> subscriber) {
      this.subscriber = subscriber;
      subscriber.onSubscribe(new QuerySubscription(this));
    }
  }

  private static class QuerySubscription implements Subscription {

    private final QueryChannelHandler queryChannelHandler;

    public QuerySubscription(QueryChannelHandler queryChannelHandler) {
      this.queryChannelHandler = queryChannelHandler;
    }

    @Override
    public void cancel() {
      queryChannelHandler.close();
    }

    @Override
    public void request(long n) {
      queryChannelHandler.request(n);
    }
  }

}