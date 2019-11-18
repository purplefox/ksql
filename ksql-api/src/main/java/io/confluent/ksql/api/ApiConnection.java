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

import io.confluent.ksql.api.protocol.ChannelHandler;
import io.confluent.ksql.api.protocol.ProtocolHandler;
import io.confluent.ksql.api.protocol.ProtocolHandler.CloseFrame;
import io.confluent.ksql.api.protocol.ProtocolHandler.DataFrame;
import io.confluent.ksql.api.protocol.ProtocolHandler.FlowFrame;
import io.confluent.ksql.api.protocol.ProtocolHandler.MessageFrame;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import java.util.HashMap;
import java.util.Map;

public abstract class ApiConnection {

  protected final ProtocolHandler protocolHandler;
  private final Map<Integer, ChannelHandler> channelHandlers = new HashMap<>();

  public interface MessageHandlerFactory {

    Runnable create(ApiConnection connection, JsonObject message);
  }

  public ApiConnection(
      Handler<Buffer> frameWriter
  ) {
    this.protocolHandler = new ProtocolHandler(this::handleMessage,
        this::handleDataFrame, this::handleFlowFrame, this::handleCloseFrame, frameWriter
    );
  }

  public void handleBuffer(Buffer buffer) {
    protocolHandler.handleBuffer(buffer);
  }

  protected abstract void runMessageHandler(Runnable messageHandler);

  protected abstract void handleMessage(MessageFrame messageFrame);

  public void writeMessage(JsonObject message) {
    protocolHandler.writeMessageFrame(message);
  }

  protected void registerChannelHandler(int channelID, ChannelHandler channelHandler) {
    channelHandlers.put(channelID, channelHandler);
  }

  private ChannelHandler getChannelHandler(int channelID) {
    ChannelHandler handler = channelHandlers.get(channelID);
    if (handler == null) {
      throw new IllegalStateException("No channel handler for channel " + channelID);
    }
    return handler;
  }

  private void handleDataFrame(DataFrame dataFrame) {
    getChannelHandler(dataFrame.channelID).handleData(dataFrame.data);
  }

  private void handleFlowFrame(FlowFrame flowFrame) {
    getChannelHandler(flowFrame.channelID).handleFlow(flowFrame.windowSize);
  }

  private void handleCloseFrame(CloseFrame closeFrame) {
    getChannelHandler(closeFrame.channelID).handleClose();
  }

}
