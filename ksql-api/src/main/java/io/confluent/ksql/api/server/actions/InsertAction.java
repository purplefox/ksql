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

import io.confluent.ksql.api.ApiConnection;
import io.confluent.ksql.api.protocol.ChannelHandler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;

public abstract class InsertAction implements ChannelHandler {

  protected final ApiConnection apiConnection;
  protected final JsonObject message;
  private int channelID;
  private Inserter inserter;

  public InsertAction(ApiConnection apiConnection, JsonObject message) {
    this.apiConnection = apiConnection;
    this.message = message;
  }

  @Override
  public void handleData(Buffer data) {
    System.out.println("Handling data in insert action on server");
    JsonObject row = new JsonObject(data);
    inserter.insertRow(row);
    System.out.println("Inserted row in inserter");
    apiConnection.writeAckFrame(channelID);
  }

  @Override
  public void handleAck() {

  }

  @Override
  public void handleFlow(int bytes) {

  }

  @Override
  public void handleClose() {

  }

  @Override
  public void run() {
    Integer channelID = message.getInteger("channel-id");
    if (channelID == null) {
      apiConnection.handleError("Message must contain a channel-id field");
      return;
    }
    this.channelID = channelID;
    String target = message.getString("target");
    if (target == null) {
      apiConnection.handleError("Message must contain a target field");
    }

    JsonObject response = new JsonObject()
        .put("type", "reply")
        .put("request-id", message.getInteger("request-id"))
        .put("status", "ok");

    inserter = createInserter(channelID, target);

    apiConnection.registerChannelHandler(channelID, this);

    apiConnection.writeMessage(response);

  }

  protected abstract Inserter createInserter(Integer channelID, String target);
}
