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
import io.confluent.ksql.api.protocol.FrameHandler;
import io.confluent.ksql.api.protocol.ProtocolHandler;
import io.confluent.ksql.api.protocol.ProtocolHandler.AckFrame;
import io.confluent.ksql.api.protocol.ProtocolHandler.CloseFrame;
import io.confluent.ksql.api.protocol.ProtocolHandler.DataFrame;
import io.confluent.ksql.api.protocol.ProtocolHandler.FlowFrame;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import java.util.HashMap;
import java.util.Map;

public abstract class ApiConnection implements FrameHandler {

  protected final ProtocolHandler protocolHandler;
  private final Map<Integer, ChannelHandler> channelHandlers = new HashMap<>();

  public interface MessageHandlerFactory {

    Runnable create(ApiConnection connection, JsonObject message);
  }

  public ApiConnection(
      Handler<Buffer> frameWriter
  ) {
    this.protocolHandler = new ProtocolHandler(this, frameWriter);
  }

  public void handleBuffer(Buffer buffer) {
    protocolHandler.handleBuffer(buffer);
  }

  protected abstract void runMessageHandler(Runnable messageHandler);

  public void writeMessage(JsonObject message) {
    protocolHandler.writeMessageFrame(message);
  }

  public void writeDataFrame(int channelID, Buffer buffer) {
    protocolHandler.writeDataFrame(channelID, buffer);
  }

  public void writeAckFrame(int channelID) {
    protocolHandler.writeAckFrame(channelID);
  }

  public void writeFlowFrame(int channelID, int bytes) {
    protocolHandler.writeFlowFrame(channelID, bytes);
  }

  public void writeCloseFrame(int channelID) {
    protocolHandler.writeCloseFrame(channelID);
  }

  public void registerChannelHandler(int channelID, ChannelHandler channelHandler) {
    channelHandlers.put(channelID, channelHandler);
  }

  public void handleError(String errMessage) {
    JsonObject response = new JsonObject().put("type", "error")
        .put("message", errMessage);
    protocolHandler.writeMessageFrame(response);
  }

  private ChannelHandler getChannelHandler(int channelID) {
    ChannelHandler handler = channelHandlers.get(channelID);
    if (handler == null) {
      throw new IllegalStateException("No channel handler for channel " + channelID);
    }
    return handler;
  }

  public void handleDataFrame(DataFrame dataFrame) {
    getChannelHandler(dataFrame.channelID).handleData(dataFrame.data);
  }

  public void handleAckFrame(AckFrame ackFrame) {
    getChannelHandler(ackFrame.channelID).handleAck();
  }

  public void handleFlowFrame(FlowFrame flowFrame) {
    getChannelHandler(flowFrame.channelID).handleFlow(flowFrame.bytes);
  }

  public void handleCloseFrame(CloseFrame closeFrame) {
    getChannelHandler(closeFrame.channelID).handleClose();
  }

}