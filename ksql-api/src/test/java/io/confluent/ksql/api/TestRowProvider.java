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

import io.confluent.ksql.api.server.actions.RowProvider;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import java.util.Iterator;
import java.util.List;

public class TestRowProvider implements RowProvider {

  private final JsonArray colNames;
  private final JsonArray colTypes;
  private final List<JsonArray> rows;
  private final int queryID;
  private final boolean terminal;

  private Iterator<JsonArray> iter;
  private int pos;

  public TestRowProvider(JsonArray colNames, JsonArray colTypes,
      List<JsonArray> rows, int queryID, boolean terminal) {
    this.colNames = colNames;
    this.colTypes = colTypes;
    this.rows = rows;
    this.queryID = queryID;
    this.terminal = terminal;
    this.iter = rows.iterator();
  }

  @Override
  public int available() {
    return rows.size() - pos;
  }

  @Override
  public synchronized Buffer poll() {
    pos++;
    JsonArray arr = iter.next();
    return arr.toBuffer();
  }

  @Override
  public void start() {
  }

  @Override
  public synchronized boolean complete() {
    if (!terminal) {
      return false;
    }
    return pos >= rows.size();
  }

  @Override
  public JsonArray colNames() {
    return colNames;
  }

  @Override
  public JsonArray colTypes() {
    return colTypes;
  }

  @Override
  public int queryID() {
    return queryID;
  }
}
