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

package io.confluent.ksql.api.server.actions;

import io.confluent.ksql.api.protocol.ChannelHandler;
import io.confluent.ksql.api.server.ServerConnection;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;

public class QueryAction implements ChannelHandler, Runnable {

  private final ServerConnection serverConnection;
  private final JsonObject message;

  public QueryAction(ServerConnection serverConnection, JsonObject message) {
    this.serverConnection = serverConnection;
    this.message = message;
    Integer channelID = message.getInteger("channel-id");
    if (channelID == null) {
      serverConnection.handleError("Message must contain a channel-id field");
      return;
    }
    String query = message.getString("query");
    if (query == null) {
      serverConnection.handleError("Control message must contain a query field");
    }
  }

  private static int queryIDSequence;

  private static synchronized int generateQueryID() {
    return queryIDSequence++;
  }

  @Override
  public void run() {

    int queryID = generateQueryID();

    JsonObject response = new JsonObject();
    response.put("request-id", message.getInteger("request-id"));
    response.put("query-id", queryID);
    response.put("status", "ok");

    serverConnection.writeMessage(response);

  }

  @Override
  public void handleData(Buffer data) {
  }

  @Override
  public void handleFlow(int windowSize) {
  }

  /*    final List<ParsedStatement> statements = ksqlEngine.parse(queryString);
    if ((statements.size() != 1)) {
      serverConnection.handleError(
          String.format("Expected exactly one KSQL statement; found %d instead", statements.size());
      return;
    }

    PreparedStatement<?> ps = ksqlEngine.prepare(statements.get(0));

    final Statement statement = ps.getStatement();

    if (!(statement instanceof Query)) {
      serverConnection.handleError("Invalid query: " + queryString);
      return;
    }

    Query query = (Query) statement;*/


}
