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
import io.confluent.ksql.api.impl.Utils;
import io.confluent.ksql.api.protocol.ChannelHandler;
import io.confluent.ksql.api.protocol.ProtocolHandler.MessageFrame;
import io.confluent.ksql.rest.server.resources.streaming.Flow.Subscriber;
import io.confluent.ksql.rest.server.resources.streaming.Flow.Subscription;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public class ClientConnection extends ApiConnection implements KSqlConnection {

  private final Map<Integer, Consumer<JsonObject>> requestMap = new ConcurrentHashMap<>();
  private int channelIDSequence;
  private int requestIDSequence;

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
  public CompletableFuture<Integer> streamQuery(String query,
      Subscriber<JsonObject> subscriber) {
    ChannelHandler handler = new QueryChannelHandler(subscriber);
    int channelID = channelIDSequence++;
    int requestID = requestIDSequence++;
    registerChannelHandler(channelID, handler);
    JsonObject message = new JsonObject()
        .put("type", "query")
        .put("query", query)
        .put("channel-id", channelID)
        .put("request-id", requestID);
    Promise<Integer> promise = Promise.promise();
    requestMap.put(requestID, jo -> handleQueryReply(promise, jo));
    writeMessage(message);
    // Race here - if tokens are requested from subscriber before message has been writen and
    // channel setup
    subscriber.onSubscribe(new QuerySubscription());
    return Utils.convertFuture(promise.future());
  }

  private static class QuerySubscription implements Subscription {

    @Override
    public void cancel() {
    }

    @Override
    public void request(long n) {
    }
  }

  @Override
  protected void handleError(String errorMessage) {
    // TODO
  }

  @Override
  protected void runMessageHandler(Runnable messageHandler) {
    messageHandler.run();
  }

  private void handleQueryReply(Promise<Integer> promise, JsonObject object) {
    Integer queryID = object.getInteger("query-id");
    if (queryID == null) {
      promise.fail(new IllegalStateException("No query-id in reply"));
    } else {
      promise.complete(queryID);
    }
  }

  private void handleReply(int requestID, JsonObject reply) {
    if (requestMap.isEmpty()) {
      throw new IllegalStateException("No requests in map");
    }
    Consumer<JsonObject> replyHandler = requestMap.get(requestID);
    replyHandler.accept(reply);
  }

  static class QueryChannelHandler implements ChannelHandler {

    private final Subscriber<JsonObject> subscriber;

    public QueryChannelHandler(Subscriber<JsonObject> subscriber) {
      this.subscriber = subscriber;
    }

    @Override
    public void handleData(Buffer data) {
      JsonObject row = new JsonObject(data);
      subscriber.onNext(row);
    }

    @Override
    public void handleFlow(int windowSize) {

    }

    @Override
    public void run() {

    }
  }

}
