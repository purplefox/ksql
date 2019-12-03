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

import static io.confluent.ksql.api.client.impl.Utils.checkReplyStatus;

import io.confluent.ksql.api.ApiConnection;
import io.confluent.ksql.api.client.KsqlDBClientException;
import io.confluent.ksql.api.client.QueryResult;
import io.confluent.ksql.api.client.Row;
import io.confluent.ksql.api.flow.Subscriber;
import io.confluent.ksql.api.flow.Subscription;
import io.confluent.ksql.api.protocol.ChannelHandler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;

class QueryChannelHandler implements ChannelHandler, QueryResult {

  private final int channelID;
  private final Queue<Buffer> incomingData = new ConcurrentLinkedQueue<>();
  private final CompletableFuture<QueryResult> future;
  private final ApiConnection apiConnection;
  private int queryID;
  private QueryResultHeader header;
  private long tokenDemand;
  private boolean closed;
  private Subscriber<Row> subscriber;

  QueryChannelHandler(int channelID,
      CompletableFuture<QueryResult> future, ApiConnection apiConnection) {
    this.channelID = channelID;
    this.future = future;
    this.apiConnection = apiConnection;
  }

  @Override
  public void handleMessage(Buffer buffer) {
    JsonObject reply = new JsonObject(buffer);
    if (checkReplyStatus(future, reply)) {
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
      this.queryID = queryID;
      header = new QueryResultHeader(columns, colTypes);
      future.complete(this);
    }
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
      apiConnection.writeFlowFrame(channelID, buffer.length());
      deliverBuffer(buffer);
      tokens--;
    }
  }

  public synchronized void close() {
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
  public int queryID() {
    return queryID;
  }

  @Override
  public Row poll() {
    return poll(Long.MAX_VALUE);
  }

  @Override
  public Row poll(long timeoutMs) {
    if (subscriber != null) {
      throw new IllegalStateException("Can't call poll if subscribed");
    }
    return null;
  }

  @Override
  public void setConsumer(Consumer<Row> consumer) {
    subscribe(new SimpleSubscriber(consumer));
  }

  @Override
  public synchronized void subscribe(Subscriber<Row> subscriber) {
    this.subscriber = subscriber;
    subscriber.onSubscribe(new QuerySubscription(this));
  }

  private static class QuerySubscription implements Subscription {

    private final QueryChannelHandler queryChannelHandler;

    QuerySubscription(QueryChannelHandler queryChannelHandler) {
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


  private static class SimpleSubscriber implements Subscriber<Row> {

    private final Consumer<Row> consumer;
    private Subscription subscription;

    SimpleSubscriber(Consumer<Row> consumer) {
      this.consumer = consumer;
    }

    @Override
    public void onNext(Row item) {
      consumer.accept(item);
      subscription.request(1);
    }

    @Override
    public void onError(Throwable e) {

    }

    @Override
    public void onComplete() {

    }

    @Override
    public synchronized void onSubscribe(Subscription subscription) {
      this.subscription = subscription;
      subscription.request(1);
    }
  }


}
