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
import io.confluent.ksql.api.client.KsqlDBConnection;
import io.confluent.ksql.api.client.KsqlDBSession;
import io.confluent.ksql.api.client.QueryResult;
import io.confluent.ksql.api.client.Row;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class ClientConnection extends ApiConnection implements KsqlDBConnection {

  private final AtomicInteger channelIDSequence = new AtomicInteger();
  private final Map<String, InsertChannelHandler> insertChannels = new ConcurrentHashMap<>();

  public ClientConnection(Handler<Buffer> frameWriter) {
    super(frameWriter);
  }

  @Override
  public KsqlDBSession session() {
    return new Session();
  }

  @Override
  public CompletableFuture<Void> executeDDL(String command) {
    return null;
  }

  @Override
  public CompletableFuture<JsonObject> describe(String target) {
    return null;
  }

  @Override
  public CompletableFuture<List<JsonObject>> listStreams() {
    return null;
  }

  @Override
  public CompletableFuture<List<JsonObject>> listTopics() {
    return null;
  }

  @Override
  public void close() {
  }

  class Session implements KsqlDBSession {

    @Override
    public CompletableFuture<QueryResult> streamQuery(String query, boolean pull) {
      int channelID = channelIDSequence.getAndIncrement();
      JsonObject message = new JsonObject()
          .put("type", "query")
          .put("query", query)
          .put("channel-id", channelID)
          .put("pull", pull);
      CompletableFuture<QueryResult> future = new CompletableFuture<>();
      QueryChannelHandler qch = new QueryChannelHandler(channelID, future, ClientConnection.this);
      registerChannelHandler(channelID, qch);
      writeRequestFrame(channelID, REQUEST_TYPE_QUERY, message);
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
        JsonObject message = new JsonObject()
            .put("type", "insert")
            .put("target", target)
            .put("channel-id", channelID);
        handler = new InsertChannelHandler(channelID, future, ClientConnection.this);
        registerChannelHandler(channelID, handler);
        insertChannels.put(target, handler);
        writeRequestFrame(channelID, REQUEST_TYPE_INSERT, message);
      }
      handler.sendInsertData(row);
      return future;
    }
  }
}