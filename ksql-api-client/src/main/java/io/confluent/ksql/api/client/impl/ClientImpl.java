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
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpVersion;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.SocketAddress;
import io.vertx.core.parsetools.RecordParser;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.reactivestreams.Publisher;

public class ClientImpl implements Client {

  private final ClientOptions clientOptions;
  private final Vertx vertx;
  private final HttpClient httpClient;
  private final SocketAddress serverSocketAddress;
  private final boolean ownedVertx;

  public ClientImpl(final ClientOptions clientOptions, final Vertx vertx) {
    this(clientOptions, vertx, false);
  }

  public ClientImpl(final ClientOptions clientOptions) {
    this(clientOptions, Vertx.vertx(), true);
  }

  private ClientImpl(final ClientOptions clientOptions, final Vertx vertx,
      final boolean ownedVertx) {
    this.clientOptions = clientOptions.copy();
    this.vertx = vertx;
    this.ownedVertx = ownedVertx;

    final HttpClientOptions options = new HttpClientOptions()
        .setSsl(clientOptions.isUseTls())
        .setUseAlpn(true)
        .setProtocolVersion(HttpVersion.HTTP_2)
        .setDefaultHost(clientOptions.getHost())
        .setDefaultPort(clientOptions.getPort())
        .setTrustAll(true);

    this.httpClient = vertx.createHttpClient(options);

    this.serverSocketAddress = io.vertx.core.net.SocketAddress
        .inetSocketAddress(clientOptions.getPort(), clientOptions.getHost());
  }

  @Override
  public CompletableFuture<QueryResult> streamQuery(final String sql, final JsonObject properties) {

    final JsonObject requestBody = new JsonObject().put("sql", sql).put("properties", properties);

    final CompletableFuture<QueryResult> cf = new CompletableFuture<>();

    httpClient.request(HttpMethod.POST,
        serverSocketAddress, clientOptions.getPort(), clientOptions.getHost(),
        "/query-stream",
        response -> handleResponse(response, cf))
        .exceptionHandler(this::handleRequestException)
        .end(requestBody.toBuffer());

    return cf;
  }

  private void handleRequestException(final Throwable t) {

  }

  private void handleResponse(final HttpClientResponse response,
      final CompletableFuture<QueryResult> cf) {
    final RecordParser recordParser = RecordParser.newDelimited("\n", response);
    final QueryResponseHandler responseHandler = new QueryResponseHandler(Vertx.currentContext(),
        recordParser, cf);
    recordParser.handler(responseHandler::handleBodyBuffer);
    recordParser.endHandler(responseHandler::handleBodyEnd);
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
    httpClient.close();
    if (ownedVertx) {
      vertx.close();
    }
  }
}
