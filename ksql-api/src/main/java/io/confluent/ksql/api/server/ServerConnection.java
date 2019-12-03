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

package io.confluent.ksql.api.server;

import io.confluent.ksql.api.ApiConnection;
import io.confluent.ksql.api.protocol.ChannelHandler;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import java.util.Map;

public class ServerConnection extends ApiConnection {

  private final Map<Short, ChannelHandlerFactory> channelHandlerFactories;

  public ServerConnection(
      Handler<Buffer> frameWriter,
      Map<Short, ChannelHandlerFactory> channelHandlerFactories
  ) {
    super(frameWriter);
    this.channelHandlerFactories = channelHandlerFactories;
  }

  @Override
  public void handleRequestFrame(int channelID, short frameType, Buffer buffer) {
    ChannelHandlerFactory factory = channelHandlerFactories.get(frameType);
    if (factory == null) {
      throw new IllegalStateException("No factory for type " + frameType);
    }
    ChannelHandler handler = factory.create(channelID, this);
    handler.handleMessage(buffer);
  }

}
