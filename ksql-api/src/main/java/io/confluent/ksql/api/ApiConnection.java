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
import io.confluent.ksql.api.protocol.WireProtocol;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import java.util.HashMap;
import java.util.Map;

public abstract class ApiConnection implements FrameHandler {

  private final WireProtocol protocolHandler;
  private final Map<Integer, ChannelHandler> channelHandlers = new HashMap<>();

  public static final short REQUEST_TYPE_QUERY = 1;
  public static final short REQUEST_TYPE_INSERT = 2;

  public interface ChannelHandlerFactory {

    ChannelHandler create(int channelID, ApiConnection connection);
  }

  public ApiConnection(
      Handler<Buffer> frameWriter
  ) {
    this.protocolHandler = new WireProtocol(this, frameWriter);
  }

  public void handleBuffer(Buffer buffer) {
    protocolHandler.handleBuffer(buffer);
  }

  public void writeRequestFrame(int channelID, short requestType, JsonObject message) {
    protocolHandler.writeRequestFrame(channelID, requestType, message);
  }

  public void writeMessageFrame(int channelID, JsonObject message) {
    protocolHandler.writeMessageFrame(channelID, message);
  }

  public void writeDataFrame(int channelID, Buffer buffer) {
    protocolHandler.writeDataFrame(channelID, buffer);
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

  public void handleError(int channelID, String errMessage) {
    JsonObject response = new JsonObject().put("type", "error")
        .put("message", errMessage);
    protocolHandler.writeMessageFrame(channelID, response);
  }

  private ChannelHandler getChannelHandler(int channelID) {
    ChannelHandler handler = channelHandlers.get(channelID);
    if (handler == null) {
      throw new IllegalStateException("No channel handler for channel " + channelID);
    }
    return handler;
  }

  @Override
  public void handleMessageFrame(int channelID, Buffer buffer) {
    getChannelHandler(channelID).handleMessage(buffer);
  }

  @Override
  public void handleDataFrame(int channelID, Buffer buffer) {
    getChannelHandler(channelID).handleData(buffer);
  }

  @Override
  public void handleFlowFrame(int channelID, int bytes) {
    getChannelHandler(channelID).handleFlow(bytes);
  }

  @Override
  public void handleCloseFrame(int channelID) {
    getChannelHandler(channelID).handleClose();
  }

  @Override
  public void handleRequestFrame(int channelID, short frameType, Buffer buffer) {
    throw new UnsupportedOperationException();
  }
}
