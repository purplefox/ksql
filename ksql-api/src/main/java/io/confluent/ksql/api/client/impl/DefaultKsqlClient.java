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

import io.confluent.ksql.api.client.KsqlDBClient;
import io.confluent.ksql.api.client.KsqlDBConnection;
import io.confluent.ksql.api.impl.Utils;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.WebSocket;
import io.vertx.core.json.JsonObject;
import java.util.concurrent.CompletableFuture;

public class DefaultKsqlClient implements KsqlDBClient {

  private final Vertx vertx;
  private final JsonObject properties;

  public DefaultKsqlClient() {
    this(Vertx.vertx(), new JsonObject());
  }

  public DefaultKsqlClient(JsonObject properties) {
    this(Vertx.vertx(), properties);
  }

  public DefaultKsqlClient(Vertx vertx, JsonObject properties) {
    this.vertx = vertx;
    this.properties = properties;
  }

  public DefaultKsqlClient(Vertx vertx) {
    this(vertx, new JsonObject());
  }

  @Override
  public CompletableFuture<KsqlDBConnection> connectWebsocket(String host, int port) {
    HttpClient client = vertx.createHttpClient();
    Promise<KsqlDBConnection> promise = Promise.promise();
    client.webSocket(port, host, "/ws-api", ar -> {
      if (ar.succeeded()) {
        WebSocket ws = ar.result();
        ClientConnection conn = new ClientConnection(buff -> {
          try {
            ws.writeBinaryMessage(buff);
            System.out.println("Wrote message from client");
          } catch (Exception e) {
            promise.fail(e);
          }
        });
        ws.handler(buff -> {
          try {
            conn.handleBuffer(buff);
          } catch (Throwable t) {
            t.printStackTrace();
            promise.fail(t);
          }
        });
        promise.complete(conn);
      } else {
        promise.fail(ar.cause());
      }
    });
    return Utils.convertFuture(promise.future());
  }

  @Override
  public CompletableFuture<KsqlDBConnection> connectTcp(String host, int port) {
    // TODO
    return null;
  }

  @Override
  public void close() {

  }
}
