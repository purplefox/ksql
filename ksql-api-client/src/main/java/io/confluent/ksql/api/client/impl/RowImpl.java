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
import io.vertx.core.json.JsonArray;
import java.util.List;

public class RowImpl implements Row {

  private final List<String> columnNames;
  private final List<String> columnTypes;
  private final JsonArray values;

  public RowImpl(final List<String> columnNames, final List<String> columnTypes,
      final JsonArray values) {
    this.columnNames = columnNames;
    this.columnTypes = columnTypes;
    this.values = values;
  }

  @Override
  public List<String> columnNames() {
    return null;
  }

  @Override
  public List<String> columnTypes() {
    return null;
  }

  @Override
  public JsonArray values() {
    return null;
  }
}
