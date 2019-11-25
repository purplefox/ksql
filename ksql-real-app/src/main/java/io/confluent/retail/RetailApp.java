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
import io.confluent.ksql.api.flow.Subscriber;
import io.confluent.ksql.api.flow.Subscription;
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
import io.vertx.ext.web.templ.thymeleaf.ThymeleafTemplateEngine;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class RetailApp extends AbstractVerticle {

  public static void main(final String[] args) {
    Launcher.executeCommand("run", RetailApp.class.getName());
  }

  private KsqlDBClient client;
  private ThymeleafTemplateEngine engine;

  @Override
  public void start(Promise<Void> startPromise) {
    client = KsqlDBClient.client();
    engine = ThymeleafTemplateEngine.create(vertx);
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
        .websocketHandler(this::handleWebsocket)
        .requestHandler(setupRouter()).listen(8080, cf);
    return cf.thenApply(s -> null);
  }

  private Router setupRouter() {
    Router router = Router.router(vertx);
    router.route(HttpMethod.GET, "/").handler(this::serveIndexPage);
    router.route(HttpMethod.GET, "/catalogue").handler(this::serveCatalogue);
    router.route(HttpMethod.GET, "/add-to-basket").handler(this::handleAddToBasket);
    router.route(HttpMethod.GET, "/basket").handler(this::serveBasket);
    router.route(HttpMethod.GET, "/orders").handler(this::serveOrders);
    router.route("/*").failureHandler(this::handleFailure);
    return router;
  }

  private synchronized CompletableFuture<KsqlDBConnection> connect() {
    return client.connectWebsocket("localhost", 8888);
  }

  void handleFailure(RoutingContext routingContext) {
    routingContext.failure().printStackTrace();
  }

  void serveIndexPage(RoutingContext routingContext) {
    routingContext.response().sendFile("web/index.html");
  }

  /*
  This is created from a pull query. Page is generated and returned from browser
   */
  void serveCatalogue(RoutingContext routingContext) {
    connect().thenCompose(conn -> conn.executeQuery("SELECT * FROM LINE_ITEM"))
        .thenAccept(lineItems -> generateCataloguePage(routingContext, lineItems))
        .exceptionally(t -> {
          t.printStackTrace();
          return null;
        });
  }

  private void generateCataloguePage(RoutingContext routingContext, List<Row> lineItems) {

    JsonObject data = new JsonObject();
    data.put("line_items", lineItems);
    engine.render(data, "templates/catalogue.html", res -> {
      if (res.succeeded()) {
        routingContext.response().end(res.result());
      } else {
        routingContext.fail(res.cause());
      }
    });
  }

  void serveBasket(RoutingContext routingContext) {
    routingContext.response().sendFile("/web/basket_ws.html");
  }

  void serveOrders(RoutingContext routingContext) {
    routingContext.response().sendFile("/web/orders_ws.html");
  }

  void handleAddToBasket(RoutingContext routingContext) {
    System.out.println("In handle add to basket");
    String itemID = routingContext.request().getParam("itemid");
    JsonObject message = new JsonObject().put("itemID", Integer.valueOf(itemID)).put("quantity", 1);
    sendToTopic("basket_events", message);
  }

  private void sendToTopic(String topicName, JsonObject message) {
    connect().thenCompose(conn -> conn.insertInto(topicName, message));
  }

  private void handleWebsocket(ServerWebSocket webSocket) {
    if (webSocket.uri().equals("/basket-ws")) {
      handleGetBasketWs(webSocket);
    } else if (webSocket.uri().equals("/orders-ws")) {
      handleGetOrders(webSocket);
    } else {
      throw new IllegalArgumentException("Invalid uri " + webSocket.uri());
    }
  }

  private void handleGetBasketWs(ServerWebSocket webSocket) {

    Subscriber<Row> subscriber = new Subscriber<Row>() {

      private Subscription subscription;

      @Override
      public synchronized void onNext(Row item) {
        System.out.println("Received result in subscriber on client");
        webSocket.writeTextMessage(item.values().toString());
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
    };

    connect()
        .thenCompose(conn -> conn
            .streamQuery("SELECT * FROM USER_BASKET WHERE USER_ID = 23 EMIT CHANGES", false,
                subscriber));
  }

  private void handleGetOrders(ServerWebSocket webSocket) {
    CompletableFuture<List<Row>> result =
        connect().thenCompose(conn -> conn.executeQuery("SELECT * FROM LINE_ITEM"));
    result.thenAccept(rows -> {
      rows.forEach(row -> webSocket.writeTextMessage(row.toString()));
    });
  }

  private void handleGetCatalogueWebsocket(ServerWebSocket webSocket) {
    CompletableFuture<List<Row>> result =
        connect().thenCompose(conn -> conn.executeQuery("SELECT * FROM LINE_ITEMS"));
    result.thenAccept(rows -> {
      rows.forEach(row -> webSocket.writeTextMessage(row.toString()));
    });
  }

}
