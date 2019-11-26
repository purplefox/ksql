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
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.templ.thymeleaf.ThymeleafTemplateEngine;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class RetailApp extends AbstractVerticle {

  public static void main(final String[] args) {
    Launcher.executeCommand("run", RetailApp.class.getName());
  }

  private KsqlDBClient client;
  private ThymeleafTemplateEngine engine;
  private volatile KsqlDBConnection connection;

  @Override
  public void start(Promise<Void> startPromise) {
    client = KsqlDBClient.client();
    engine = ThymeleafTemplateEngine.create(vertx);
    client.connectWebsocket("localhost", 8888)
        .thenCompose(conn -> {
          connection = conn;
          return startServer();
        })
        .whenComplete((v, t) -> {
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
    router.route(HttpMethod.GET, "/place-order").handler(this::handlePlaceOrder);
    router.route(HttpMethod.GET, "/basket").handler(this::serveBasket);
    router.route(HttpMethod.GET, "/orders-report").handler(this::serveOrdersReport);
    router.route(HttpMethod.GET, "/orders").handler(this::serveOrders);
    router.route("/*").failureHandler(this::handleFailure);
    return router;
  }

  private synchronized CompletableFuture<KsqlDBConnection> connect() {
    if (connection != null) {
      return CompletableFuture.completedFuture(connection);
    } else {
      return client.connectWebsocket("localhost", 8888);
    }
  }

  private void handleFailure(RoutingContext routingContext) {
    routingContext.failure().printStackTrace();
  }

  private void serveIndexPage(RoutingContext routingContext) {
    routingContext.response().sendFile("web/index.html");
  }

  /*
  This is created from a pull query. Page is generated and returned from browser
   */
  private void serveCatalogue(RoutingContext routingContext) {
    connection.executeQuery("SELECT * FROM LINE_ITEM")
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

  private void serveBasket(RoutingContext routingContext) {
    routingContext.response().sendFile("/web/basket_ws.html");
  }

  private void serveOrders(RoutingContext routingContext) {
    routingContext.response().sendFile("/web/orders_ws.html");
  }

  private void serveOrdersReport(RoutingContext routingContext) {
    routingContext.response().sendFile("/web/orders_report_ws.html");
  }

  private void handleAddToBasket(RoutingContext routingContext) {
    System.out.println("In handle add to basket");
    String itemID = routingContext.request().getParam("itemid");
    String itemName = routingContext.request().getParam("itemname");
    String itemPrice = routingContext.request().getParam("itemprice");
    JsonObject message = new JsonObject()
        .put("userID", 23)
        .put("itemID", Integer.valueOf(itemID)).put("quantity", 1)
        .put("itemName", itemName)
        .put("itemPrice", Double.valueOf(itemPrice))
        .put("amount", 1);
    sendToTopic("basket_events", message);
  }

  private void handlePlaceOrder(RoutingContext routingContext) {
    System.out.println("In handle place order");
    int userID = 23;
    connection
        .executeQuery("SELECT * FROM USER_BASKET WHERE USER_ID = 23")
        .thenApply(rows -> {
          List<JsonArray> list = rows.stream().map(Row::values).collect(Collectors.toList());
          JsonArray items = new JsonArray(list);
          JsonObject order = new JsonObject();
          order.put("items", items);
          order.put("userID", userID);
          return order;
        })
        .thenAccept(order -> {
          System.out.println("Placing order: " + order);
          sendToTopic("order_event", order);
        });
  }

  private void sendToTopic(String topicName, JsonObject message) {
    connection.insertInto(topicName, message);
  }

  private void handleWebsocket(ServerWebSocket webSocket) {
    if (webSocket.uri().equals("/basket-ws")) {
      handleGetBasketWs(webSocket, 23);
    } else if (webSocket.uri().equals("/orders-report-ws")) {
      handleGetOrderCount(webSocket);
    } else if (webSocket.uri().equals("/orders-ws")) {
      handleGetOrders(webSocket);
    } else {
      throw new IllegalArgumentException("Invalid uri " + webSocket.uri());
    }
  }

  private void handleGetBasketWs(ServerWebSocket webSocket, int userID) {
    connection
        .streamQuery("SELECT * FROM USER_BASKET WHERE USER_ID = " + userID + " EMIT CHANGES", false,
            new QuerySubscriber(webSocket));
  }

  private void handleGetOrderCount(ServerWebSocket webSocket) {
    connection.streamQuery("SELECT * FROM ORDER_REPORT EMIT CHANGES", false,
        new QuerySubscriber(webSocket));
  }

  private void handleGetOrders(ServerWebSocket webSocket) {
    connection.streamQuery("SELECT * FROM ORDER_EVENT EMIT CHANGES", false,
        new QuerySubscriber(webSocket));
  }

}
