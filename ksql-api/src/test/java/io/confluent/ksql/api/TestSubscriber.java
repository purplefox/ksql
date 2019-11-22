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

package io.confluent.ksql.api;

import io.confluent.ksql.api.flow.Subscriber;
import io.confluent.ksql.api.flow.Subscription;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

public class TestSubscriber<T> implements Subscriber<T> {

  private final List<T> items = new ArrayList<>();
  private Throwable error;
  private boolean completed;
  private Subscription subscription;

  @Override
  public synchronized void onNext(T item) {
    if (subscription == null) {
      throw new IllegalStateException("subscription has not been set");
    }
    subscription.request(1);
    items.add(item);
  }

  @Override
  public synchronized void onError(Throwable e) {
    error = e;
  }

  @Override
  public synchronized void onComplete() {
    completed = true;
  }

  @Override
  public synchronized void onSubscribe(Subscription subscription) {
    this.subscription = subscription;
    subscription.request(1);
  }

  public List<T> waitForItems(int num, long timeout) throws Exception {
    long start = System.currentTimeMillis();
    while ((System.currentTimeMillis() - start) < timeout) {
      synchronized (this) {
        if (items.size() >= num) {
          return items;
        }
        Thread.sleep(10);
      }
    }
    throw new TimeoutException("Timed out waiting for items");
  }

  public boolean isCompleted() {
    return completed;
  }
}
