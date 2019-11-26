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

import io.confluent.ksql.api.server.actions.Inserter;
import io.vertx.core.json.JsonObject;
import java.util.ArrayList;
import java.util.List;

public class TestInserter implements Inserter {

  private List<JsonObject> rows = new ArrayList<>();

  @Override
  public synchronized void insertRow(JsonObject row) {
    rows.add(row);
  }

  public List<JsonObject> getRows() {
    return rows;
  }
}