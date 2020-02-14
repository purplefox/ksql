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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.ksql.api.client.QueryResult;
import io.confluent.ksql.api.client.Row;
import io.confluent.ksql.api.common.QueryResponseMetadata;
import io.confluent.ksql.api.common.Utils;
import io.vertx.core.Context;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.jackson.DatabindCodec;
import io.vertx.core.parsetools.RecordParser;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

class QueryResponseHandler {

  private final Context context;
  private final RecordParser recordParser;
  private final CompletableFuture<QueryResult> cf;
  private boolean hasReadArguments;
  private QueryResultImpl queryResult;
  private boolean paused;

  QueryResponseHandler(final Context context, final RecordParser recordParser,
      final CompletableFuture<QueryResult> cf) {
    this.context = context;
    this.recordParser = recordParser;
    this.cf = cf;
  }

  public void handleBodyBuffer(final Buffer buff) {
    checkContext();
    if (!hasReadArguments) {
      handleArgs(buff);
    } else if (queryResult != null) {
      handleRow(buff);
    }
  }

  private void handleArgs(final Buffer buff) {
    hasReadArguments = true;

    final QueryResponseMetadata queryResponseMetadata;
    final ObjectMapper objectMapper = DatabindCodec.mapper();
    try {
      queryResponseMetadata = objectMapper
          .readValue(buff.getBytes(), QueryResponseMetadata.class);
    } catch (Exception e) {
      cf.completeExceptionally(e);
      return;
    }

    this.queryResult = new QueryResultImpl(context, queryResponseMetadata.queryId,
        Collections.unmodifiableList(queryResponseMetadata.columnNames),
        Collections.unmodifiableList(queryResponseMetadata.columnTypes));
    cf.complete(queryResult);
  }

  private void handleRow(final Buffer buff) {
    final JsonArray values = new JsonArray(buff);
    final Row row = new RowImpl(queryResult.columnNames(), queryResult.columnTypes(), values);
    final boolean full = queryResult.accept(row);
    if (full && !paused) {
      recordParser.pause();
      queryResult.drainHandler(this::publisherReceptive);
      paused = true;
    }
  }

  private void publisherReceptive() {
    paused = false;
    recordParser.resume();
  }

  public void handleBodyEnd(final Void v) {
    checkContext();
  }

  private void checkContext() {
    Utils.checkContext(context);
  }
}
