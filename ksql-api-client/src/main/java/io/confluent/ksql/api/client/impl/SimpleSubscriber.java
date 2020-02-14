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

import io.confluent.ksql.api.client.Row;
import io.confluent.ksql.api.server.BaseSubscriber;
import io.vertx.core.Context;
import java.util.function.Consumer;
import org.reactivestreams.Subscription;

class SimpleSubscriber extends BaseSubscriber<Row> {

  private static final int REQUEST_BATCH_SIZE = 100;

  private final Consumer<Row> consumer;
  private int tokens;
  private boolean complete;

  SimpleSubscriber(final Context context, final Consumer<Row> consumer) {
    super(context);
    this.consumer = consumer;
  }

  @Override
  protected void afterSubscribe(final Subscription subscription) {
    checkRequestTokens();
  }

  @Override
  protected void handleValue(final Row row) {
    tokens--;
    checkRequestTokens();
    consumer.accept(row);
  }

  @Override
  protected void handleComplete() {
  }

  @Override
  protected void handleError(final Throwable t) {
  }

  synchronized boolean isComplete() {
    return complete;
  }

  private void checkRequestTokens() {
    if (tokens == 0) {
      tokens += REQUEST_BATCH_SIZE;
      makeRequest(REQUEST_BATCH_SIZE);
    }
  }
}

