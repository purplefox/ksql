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

import io.confluent.ksql.api.server.actions.QueryAction;
import io.confluent.ksql.api.server.actions.RowProvider;
import io.vertx.core.Vertx;

public class TestQueryAction extends QueryAction {

  private final RowProvider rowProvider;

  public TestQueryAction(int channelID, ApiConnection apiConnection,
      Vertx vertx, RowProvider rowProvider) {
    super(channelID, apiConnection, vertx);
    this.rowProvider = rowProvider;
  }

  @Override
  protected RowProvider createRowProvider(String queryString) {
    return rowProvider;
  }
}
