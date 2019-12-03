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

package io.confluent.ksql.api.client.impl;

import io.confluent.ksql.api.ApiConnection;
import io.confluent.ksql.api.protocol.ChannelHandler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;

// TODO the channel handler common stuff - outgoing and incoming buffers, tokens etc
// can be abstracted out into a Channel class
class InsertChannelHandler implements ChannelHandler {

  private final int channelID;
  private final CompletableFuture<Void> future;
  private final ApiConnection apiConnection;
  private int bytes = 1024 * 1024; // Initial window size

  private final Queue<Buffer> outgoingInserts = new ConcurrentLinkedQueue<>();

  public InsertChannelHandler(int channelID, CompletableFuture<Void> future,
      ApiConnection apiConnection) {
    this.channelID = channelID;
    this.future = future;
    this.apiConnection = apiConnection;
  }

  void sendInsertData(JsonObject row) {
    Buffer data = row.toBuffer();
    if (bytes >= data.length()) {
      bytes -= data.length();
      apiConnection.writeDataFrame(channelID, data);
    } else {
      outgoingInserts.add(data);
    }
  }

  private void checkSendOutgoing() {
    while (!outgoingInserts.isEmpty()) {
      Buffer data = outgoingInserts.peek();
      if (bytes >= data.length()) {
        outgoingInserts.remove();
        sendBuffer(data);
      } else {
        break;
      }
    }
  }

  private void sendBuffer(Buffer data) {
    bytes -= data.length();
    apiConnection.writeDataFrame(channelID, data);
  }

  @Override
  public void handleMessage(Buffer message) {
  }

  @Override
  public void handleData(Buffer data) {
  }

  @Override
  public synchronized void handleFlow(int bytes) {
    this.bytes += bytes;
    checkSendOutgoing();
  }

  @Override
  public void handleClose() {
  }

}
