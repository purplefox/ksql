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

package io.confluent.server;

import io.confluent.retail.RetailApp;
import io.vertx.core.Launcher;

public class Starter {

  public static void main(String[] args) {
    try {
      FakeKsqlDBServer server = new FakeKsqlDBServer();
      server.start().thenRun(() -> {
        Launcher.executeCommand("run", RetailApp.class.getName());
      }).get();
      System.in.read();
    } catch (Throwable t) {
      t.printStackTrace();
    }
  }
}
