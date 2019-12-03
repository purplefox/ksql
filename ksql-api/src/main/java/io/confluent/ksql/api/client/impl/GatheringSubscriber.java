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

import io.confluent.ksql.api.client.Row;
import io.confluent.ksql.api.flow.Subscriber;
import io.confluent.ksql.api.flow.Subscription;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

class GatheringSubscriber implements Subscriber<Row> {

  private final CompletableFuture<List<Row>> futRes;
  private final List<Row> rows = new ArrayList<>();
  private Subscription subscription;

  GatheringSubscriber(CompletableFuture<List<Row>> futRes) {
    this.futRes = futRes;
  }

  @Override
  public synchronized void onNext(Row item) {
    rows.add(item);
    subscription.request(1);
  }

  @Override
  public void onError(Throwable e) {
    futRes.completeExceptionally(e);
  }

  @Override
  public synchronized void onComplete() {
    futRes.complete(rows);
  }

  @Override
  public synchronized void onSubscribe(Subscription subscription) {
    this.subscription = subscription;
    subscription.request(1);
  }
}


