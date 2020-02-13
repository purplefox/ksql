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

package io.confluent.ksql.api.client;

import io.confluent.ksql.api.client.impl.ClientImpl;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.reactivestreams.Publisher;

public interface Client {

  CompletableFuture<QueryResult> streamQuery(String sql);

  CompletableFuture<List<Row>> executeQuery(String sql);

  CompletableFuture<Void> insertInto(String streamName, JsonObject row);

  Publisher<JsonObject> streamInserts(Publisher<JsonArray> insertsPublisher);

  void close();

  static Client create(ClientOptions clientOptions) {
    return new ClientImpl(clientOptions);
  }
}
