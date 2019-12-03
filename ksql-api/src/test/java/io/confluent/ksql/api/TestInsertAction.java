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

import io.confluent.ksql.api.server.actions.InsertAction;
import io.confluent.ksql.api.server.actions.Inserter;

public class TestInsertAction extends InsertAction {

  private final Inserter inserter;

  public TestInsertAction(int channelID, ApiConnection apiConnection, Inserter inserter) {
    super(channelID, apiConnection);
    this.inserter = inserter;
  }

  @Override
  protected Inserter createInserter(Integer channelID, String target) {
    return inserter;
  }
}
