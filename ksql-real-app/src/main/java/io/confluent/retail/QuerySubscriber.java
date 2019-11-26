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

import io.confluent.ksql.api.client.Row;
import io.confluent.ksql.api.flow.Subscriber;
import io.confluent.ksql.api.flow.Subscription;
import io.vertx.core.http.ServerWebSocket;

public class QuerySubscriber implements Subscriber<Row> {

  private final ServerWebSocket webSocket;
  private Subscription subscription;

  public QuerySubscriber(ServerWebSocket webSocket) {
    this.webSocket = webSocket;
  }

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
}
