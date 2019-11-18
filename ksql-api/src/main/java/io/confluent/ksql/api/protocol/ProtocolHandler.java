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

package io.confluent.ksql.api.protocol;

import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;

public class ProtocolHandler {

  private static final int MSG = 1836279562; // "msg\n"
  private static final int DAT = 1684108298; // "dat\n"
  private static final int FLO = 1912602624; // "flo\n"

  private final Handler<MessageFrame> messageHandler;
  private final Handler<DataFrame> dataHandler;
  private final Handler<FlowFrame> flowHandler;
  private final Handler<Buffer> frameWriter;

  public ProtocolHandler(
      final Handler<MessageFrame> messageHandler,
      final Handler<DataFrame> dataHandler,
      final Handler<FlowFrame> flowHandler,
      final Handler<Buffer> frameWriter
  ) {
    this.messageHandler = messageHandler;
    this.dataHandler = dataHandler;
    this.flowHandler = flowHandler;
    this.frameWriter = frameWriter;
  }

  public void writeMessageFrame(JsonObject payload) {
    Buffer payloadBuffer = payload.toBuffer();
    Buffer buff = Buffer.buffer(4 + payloadBuffer.length())
        .setInt(0, MSG)
        .setBuffer(4, payloadBuffer);
    frameWriter.handle(buff);
  }

  public void writeDataFrame(int channelID, Buffer data) {
    Buffer buff = Buffer.buffer(8 + data.length())
        .setInt(0, DAT)
        .setInt(4, channelID)
        .setBuffer(8, data);
    frameWriter.handle(buff);
  }

  public void writeFlowFrame(int channelID, int windowSize) {
    Buffer buff = Buffer.buffer(16)
        .setInt(0, FLO)
        .setInt(4, channelID)
        .setInt(8, windowSize);
    frameWriter.handle(buff);
  }

  public void handleBuffer(Buffer buffer) {
    if (buffer.length() < 8) {
      throw new IllegalStateException("Not enough data");
    }
    int type = buffer.getInt(0);
    switch (type) {
      case MSG: {
        handleMessageFrame(buffer);
        break;
      }
      case DAT: {
        handleData(buffer);
        break;
      }
      case FLO: {
        handleFlow(buffer);
        break;
      }
      default:
        throw new IllegalStateException("Invalid type " + type);
    }
  }

  private void handleMessageFrame(Buffer buffer) {
    Buffer sliced = buffer.slice(4, buffer.length());
    JsonObject payload = new JsonObject(sliced);
    System.out.println("Got message on server: " + payload);
    messageHandler.handle(new MessageFrame(payload));
  }

  private void handleData(Buffer buffer) {
    int channelID = buffer.getInt(4);
    Buffer data = buffer.slice(8, buffer.length());
    dataHandler.handle(new DataFrame(channelID, data));
  }

  private void handleFlow(Buffer buffer) {
    int channelID = buffer.getInt(4);
    int windowSize = buffer.getInt(8);
    flowHandler.handle(new FlowFrame(channelID, windowSize));
  }

  public static void main(String[] args) {
    System.out.println(toInt('m', 's', 'g', '\n'));
    System.out.println(toInt('d', 'a', 't', '\n'));
    System.out.println(toInt('f', 'l', 'o', '\n'));
  }

  private static int toInt(char c1, char c2, char c3, char c4) {
    return (((byte) c1) << 24) + (((byte) c2) << 16) + (((byte) c3) << 8) + ((byte) c4);
  }

  public static class MessageFrame {

    public final JsonObject payload;

    MessageFrame(final JsonObject payload) {
      this.payload = payload;
    }
  }

  public static class DataFrame {

    public final int channelID;
    public final Buffer data;

    DataFrame(final int channelID, final Buffer data) {
      this.channelID = channelID;
      this.data = data;
    }
  }

  public static class FlowFrame {

    public final int channelID;
    public final int windowSize;

    FlowFrame(final int channelID, final int windowSize) {
      this.channelID = channelID;
      this.windowSize = windowSize;
    }
  }
}
