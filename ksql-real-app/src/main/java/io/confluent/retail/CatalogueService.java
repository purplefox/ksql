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

package io.confluent.retail;

import io.confluent.ksql.api.client.KsqlDBClient;
import io.confluent.ksql.api.client.KsqlDBConnection;
import io.confluent.ksql.api.client.Row;
import io.confluent.ksql.api.impl.VertxCompletableFuture;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Launcher;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class CatalogueService extends AbstractVerticle {

  public static void main(final String[] args) {
    Launcher.executeCommand("run", CatalogueService.class.getName());
  }

  private KsqlDBClient client;

  @Override
  public void start(Promise<Void> startPromise) {
    startServer().whenComplete((v, t) -> {
      if (t != null) {
        startPromise.fail(t);
      } else {
        startPromise.complete();
      }
    });
  }

  private CompletableFuture<Void> startServer() {
    VertxCompletableFuture<HttpServer> cf = new VertxCompletableFuture<>();
    vertx.createHttpServer()
        .websocketHandler(this::handleGetCatalogueWebsocket)
        .requestHandler(setupRouter()).listen(8080, cf);
    return cf.thenApply(s -> null);
  }

  private Router setupRouter() {
    Router router = Router.router(vertx);
    router.route(HttpMethod.GET, "/catalogue-ws").handler(this::handleServeCataloguePage);
    router.route(HttpMethod.POST, "/add-to-basket").handler(this::handleAddToBasket);
    return router;
  }

  private synchronized CompletableFuture<KsqlDBConnection> connect() {
    return client.connectWebsocket("localhost", 8888);
  }

  void handleServeCataloguePage(RoutingContext routingContext) {
    routingContext.response().sendFile("catalogue_ws.html");
  }

  void handleAddToBasket(RoutingContext routingContext) {
    String itemID = routingContext.pathParam("itemid");
    Integer quantity = Integer.valueOf(routingContext.pathParam("quantity"));
    JsonObject message = new JsonObject().put("itemID", itemID).put("quantity", quantity);
    sendToTopic("basket_events", message);
  }

  private void sendToTopic(String topicName, JsonObject message) {
    connect().thenCompose(conn -> conn.insertInto(topicName, message));
  }

  private void handleGetCatalogueWebsocket(ServerWebSocket webSocket) {
    CompletableFuture<List<Row>> result =
        connect().thenCompose(conn -> conn.executeQuery("SELECT * FROM LINE_ITEMS"));
    result.thenAccept(rows -> {
      rows.forEach(row -> webSocket.writeTextMessage(row.toString()));
    });
  }

}
