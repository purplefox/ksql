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

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.reactivestreams.Publisher;

public interface QueryResult extends Publisher<Row> {

  List<String> columnNames();

  List<String> columnTypes();

  String queryID();

  Row poll();

  Row poll(long timeout, TimeUnit timeUnit);

  boolean isComplete();

  void close();

}
