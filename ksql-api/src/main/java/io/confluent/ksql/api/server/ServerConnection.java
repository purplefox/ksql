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
import io.confluent.ksql.api.protocol.ProtocolHandler.MessageFrame;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import java.util.Map;
import java.util.concurrent.Executor;

public class ServerConnection extends ApiConnection {

  private final Executor actionExecutor;
  private final Map<String, MessageHandlerFactory> messageHandlerFactories;

  public ServerConnection(
      Handler<Buffer> frameWriter,
      Executor actionExecutor,
      Map<String, MessageHandlerFactory> messageHandlerFactories
  ) {
    super(frameWriter);
    this.actionExecutor = actionExecutor;
    this.messageHandlerFactories = messageHandlerFactories;
  }

  @Override
  protected void handleMessage(MessageFrame messageFrame) {
    JsonObject message = messageFrame.payload;
    String type = message.getString("type");
    if (type == null) {
      handleError("Message must contain a type field");
      return;
    }
    MessageHandlerFactory factory = messageHandlerFactories.get(type);
    if (factory == null) {
      throw new IllegalStateException("No factory for type " + type);
    }
    Runnable handler = factory.create(this, message);
    runMessageHandler(handler);
  }


  @Override
  protected void runMessageHandler(Runnable messageHandler) {
    actionExecutor.execute(messageHandler);
  }

}
