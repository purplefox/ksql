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

package io.confluent.ksql.api.server;

import io.confluent.ksql.api.ApiConnection.MessageHandlerFactory;
import io.confluent.ksql.api.impl.Utils;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.ServerWebSocket;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ApiServer {

  private ExecutorService executorService;

  private final Map<String, MessageHandlerFactory> messageHandlerFactories;
  private final Vertx vertx;

  public ApiServer(Map<String, MessageHandlerFactory> messageHandlerFactories,
      Vertx vertx) {
    this.messageHandlerFactories = messageHandlerFactories;
    this.vertx = vertx;
  }

  public CompletableFuture<Void> start() {
    executorService = Executors.newFixedThreadPool(100);
    System.out.println("Creating vertx");
    vertx.exceptionHandler(Throwable::printStackTrace);
    System.out.println("Created vertx");
    Promise<HttpServer> promise = Promise.promise();
    vertx.createHttpServer().websocketHandler(this::handleWebsocket)
        .listen(8888, promise);
    Future<HttpServer> fut = promise.future();
    return Utils.convertFuture(fut.map(s -> null));
  }

  private void handleWebsocket(ServerWebSocket serverWebSocket) {
    ServerConnection conn = new ServerConnection(buff -> {
      serverWebSocket.write(buff);
      System.out.println("Wrote buffer from server");
    }, executorService, messageHandlerFactories);
    serverWebSocket.handler(buff -> {
      try {
        conn.handleBuffer(buff);
      } catch (Exception e) {
        e.printStackTrace();
      }
    });
  }

}
