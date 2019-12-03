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

import io.confluent.ksql.api.client.KsqlDBClient;
import io.confluent.ksql.api.client.KsqlDBConnection;
import io.confluent.ksql.api.client.KsqlDBSession;
import io.confluent.ksql.api.client.QueryResult;
import io.confluent.ksql.api.client.Row;
import java.util.List;

public class Examples {

  public void queryStreamAsync() {
    KsqlDBClient client = KsqlDBClient.client();
    client.connectWebsocket("localhost", 8080)
        .thenCompose(
            con -> con.session().streamQuery("select * from line_items", false))
        .thenAccept(queryResult -> {
          queryResult.setConsumer(System.out::println);
        })
        .exceptionally(this::handleException);
  }

  public void queryStreamBlocking() throws Exception {
    KsqlDBClient client = KsqlDBClient.client();
    KsqlDBConnection conn = client.connectWebsocket("localhost", 8080).get();
    KsqlDBSession session = conn.session();
    QueryResult queryResult = session.streamQuery("select * from line_items", false).get();
    Row row;
    while ((row = queryResult.poll()) != null) {
      System.out.println(row);
    }
  }

  public void queryExecuteAsync() {
    KsqlDBClient client = KsqlDBClient.client();
    client.connectWebsocket("localhost", 8080)
        .thenCompose(
            con -> con.session().executeQuery("select * from line_items"))
        .thenAccept(results -> {
          for (Row row : results) {
            System.out.println(row);
          }
        })
        .exceptionally(this::handleException);
  }

  public void queryExecuteSync() throws Exception {
    KsqlDBClient client = KsqlDBClient.client();
    KsqlDBConnection conn = client.connectWebsocket("localhost", 8080).get();
    KsqlDBSession session = conn.session();
    List<Row> results = session.executeQuery("select * from line_items").get();
    for (Row row : results) {
      System.out.println(row);
    }
  }

  Void handleException(Throwable t) {
    t.printStackTrace();
    return null;
  }

}
