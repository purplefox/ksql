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
import io.confluent.ksql.api.client.KSqlConnection;
import io.confluent.ksql.api.client.Row;
import io.confluent.ksql.api.flow.Subscriber;
import io.confluent.ksql.api.flow.Subscription;
import io.confluent.ksql.api.impl.Utils;
import io.confluent.ksql.api.protocol.ChannelHandler;
import io.confluent.ksql.api.protocol.ProtocolHandler.MessageFrame;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
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
public class ClientConnection extends ApiConnection implements KSqlConnection {

  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling
  private final Map<Integer, Consumer<JsonObject>> requestMap = new ConcurrentHashMap<>();
  private final AtomicInteger channelIDSequence = new AtomicInteger();
  private final AtomicInteger requestIDSequence = new AtomicInteger();

  public ClientConnection(Handler<Buffer> frameWriter) {
    super(frameWriter);
  }

  @Override
  protected void handleMessage(MessageFrame messageFrame) {
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

  @Override
  public CompletableFuture<Integer> streamQuery(String query, boolean pull,
      Subscriber<Row> subscriber) {
    int channelID = channelIDSequence.getAndIncrement();
    int requestID = requestIDSequence.getAndIncrement();

    JsonObject message = new JsonObject()
        .put("type", "query")
        .put("query", query)
        .put("channel-id", channelID)
        .put("request-id", requestID)
        .put("pull", pull);
    Promise<Integer> promise = Promise.promise();
    requestMap.put(requestID, jo -> handleQueryReply(promise, jo, subscriber, channelID));
    writeMessage(message);
    return Utils.convertFuture(promise.future());
  }

  @Override
  public CompletableFuture<List<Row>> executeQuery(String query) {
    CompletableFuture<List<Row>> futRes = new CompletableFuture<>();
    CompletableFuture<Integer> fut = streamQuery(query, true, new GatheringSubscriber(futRes));
    fut.exceptionally(t -> {
      futRes.completeExceptionally(t);
      return -1;
    });
    return futRes;
  }

  @Override
  protected void runMessageHandler(Runnable messageHandler) {
    messageHandler.run();
  }

  private void handleQueryReply(Promise<Integer> promise, JsonObject reply,
      Subscriber<Row> subscriber, int channelID) {
    String status = reply.getString("status");
    if (status == null) {
      throw new IllegalStateException("No status in reply");
    }
    if ("ok".equals(status)) {
      Integer queryID = reply.getInteger("query-id");
      if (queryID == null) {
        promise.fail(new IllegalStateException("No query-id in reply"));
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

      QueryChannelHandler handler = new QueryChannelHandler(subscriber, channelID,
          new QueryResultHeader(columns, colTypes));
      registerChannelHandler(channelID, handler);
      subscriber.onSubscribe(new QuerySubscription(handler));

      promise.complete(queryID);
    } else if ("err".equals(status)) {
      String errMessage = reply.getString("err-msg");
      if (errMessage == null) {
        throw new IllegalStateException("No err-msg in err reply");
      }
      promise.fail(errMessage);
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
    }
  }

  class QueryChannelHandler implements ChannelHandler {

    private final Subscriber<Row> subscriber;
    private final int channelID;
    private final Queue<Buffer> incomingData = new ConcurrentLinkedQueue<>();
    private final QueryResultHeader header;
    private long tokenDemand;
    private boolean closed;

    QueryChannelHandler(Subscriber<Row> subscriber, int channelID,
        QueryResultHeader queryResultHeader) {
      this.subscriber = subscriber;
      this.channelID = channelID;
      this.header = queryResultHeader;
    }

    @Override
    public synchronized void handleData(Buffer buffer) {
      if (closed) {
        return;
      }
      if (tokenDemand > 0) {
        deliverBuffer(buffer);
      } else {
        incomingData.add(buffer);
      }
    }

    synchronized void deliverBuffer(Buffer buffer) {
      JsonArray jsonArray = new JsonArray(buffer);
      Row row = new RowImpl(jsonArray, header);
      tokenDemand--;
      subscriber.onNext(row);
    }

    synchronized void request(long n) {
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
      subscriber.onComplete();
    }

    @Override
    public void run() {
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