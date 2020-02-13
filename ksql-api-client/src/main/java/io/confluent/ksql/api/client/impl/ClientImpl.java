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

package io.confluent.ksql.api.client.impl;

import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ClientOptions;
import io.confluent.ksql.api.client.QueryResult;
import io.confluent.ksql.api.client.Row;
import io.vertx.codegen.annotations.Nullable;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpVersion;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.streams.WriteStream;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.ext.web.codec.BodyCodec;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.reactivestreams.Publisher;

public class ClientImpl implements Client {

  private final ClientOptions clientOptions;
  private final Vertx vertx;
  private final WebClient webClient;

  public ClientImpl(final ClientOptions clientOptions) {

    this.clientOptions = clientOptions.copy();
    this.vertx = Vertx.vertx();

    final WebClientOptions options = new WebClientOptions()
        .setSsl(clientOptions.isUseTls())
        .setUseAlpn(true)
        .setProtocolVersion(HttpVersion.HTTP_2);

    this.webClient = WebClient.create(vertx, options);
  }


  @Override
  public CompletableFuture<QueryResult> streamQuery(final String sql, final JsonObject properties) {

    final JsonObject requestBody = new JsonObject().put("sql", sql).put("properties", properties);

    webClient.post(clientOptions.getPort(), clientOptions.getHost(), "/query-stream")
        .as(BodyCodec.pipe(writeStream))
        .sendJsonObject(requestBody, ar -> {
        });

    return null;
  }

  class ResponseStream implements WriteStream<Buffer> {

    @Override
    public WriteStream<Buffer> exceptionHandler(final Handler<Throwable> handler) {
      return null;
    }

    @Override
    public WriteStream<Buffer> write(final Buffer data) {
      return null;
    }

    @Override
    public WriteStream<Buffer> write(final Buffer data, final Handler<AsyncResult<Void>> handler) {
      return null;
    }

    @Override
    public void end() {

    }

    @Override
    public void end(final Handler<AsyncResult<Void>> handler) {

    }

    @Override
    public WriteStream<Buffer> setWriteQueueMaxSize(final int maxSize) {
      return null;
    }

    @Override
    public boolean writeQueueFull() {
      return false;
    }

    @Override
    public WriteStream<Buffer> drainHandler(@Nullable final Handler<Void> handler) {
      return null;
    }
  }

  @Override
  public CompletableFuture<List<Row>> executeQuery(final String sql) {
    return null;
  }

  @Override
  public CompletableFuture<Void> insertInto(final String target, final JsonObject row) {
    return null;
  }

  @Override
  public Publisher<JsonObject> streamInserts(final Publisher<JsonArray> insertsPublisher) {
    return null;
  }

  @Override
  public void close() {

  }
}
