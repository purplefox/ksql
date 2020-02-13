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

import io.confluent.ksql.api.client.QueryResult;
import io.confluent.ksql.api.client.Row;
import io.confluent.ksql.api.common.BufferedPublisher;
import io.vertx.core.Context;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

class QueryResultImpl extends BufferedPublisher<Row> implements QueryResult {

  private final String queryId;
  private final List<String> columnNames;
  private final List<String> columnTypes;
  private final PollableSubscriber pollableSubscriber;
  private boolean polling;
  private SimpleSubscriber simpleSubscriber;

  QueryResultImpl(final Context context, final String queryId, final List<String> columnNames,
      final List<String> columnTypes) {
    super(context);
    this.queryId = queryId;
    this.columnNames = columnNames;
    this.columnTypes = columnTypes;
    this.pollableSubscriber = new PollableSubscriber(ctx);
  }

  @Override
  public List<String> columnNames() {
    return columnNames;
  }

  @Override
  public List<String> columnTypes() {
    return columnTypes;
  }

  @Override
  public String queryID() {
    return queryId;
  }

  @Override
  public Row poll() {
    return poll(0, TimeUnit.MILLISECONDS);
  }

  @Override
  public synchronized Row poll(final long timeout, final TimeUnit timeUnit) {
    if (simpleSubscriber != null) {
      throw new IllegalStateException("Cannot poll if consumer has been set");
    }
    if (!polling) {
      subscribe(pollableSubscriber);
      polling = true;
    }
    return pollableSubscriber.poll(timeout, timeUnit);
  }

  @Override
  public synchronized void setConsumer(final Consumer<Row> consumer) {
    if (polling) {
      throw new IllegalStateException("Cannot set consumer if has been used for polling");
    }
    this.simpleSubscriber = new SimpleSubscriber(ctx, consumer);
    subscribe(simpleSubscriber);
  }

  @Override
  public void close() {
    // Outside the lock in case thread is polling
    pollableSubscriber.close();
    synchronized (this) {
      if (simpleSubscriber != null) {
        simpleSubscriber.cancel();
      }
    }
  }


}
