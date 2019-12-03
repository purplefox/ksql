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

/*
TODO heartbeats
 */
public class WireProtocol {

  public static final int MAGIC_NUMBER = 1802727788; // 'ksql'
  public static final int VERSION = 1;

  public static final int FRAME_TYPE_REQ = 1;
  public static final int FRAME_TYPE_MSG = 2;
  public static final int FRAME_TYPE_DAT = 3;
  public static final int FRAME_TYPE_FLO = 4;
  public static final int FRAME_TYPE_CLS = 5;

  private final FrameHandler frameHandler;
  private final Handler<Buffer> frameWriter;

  public WireProtocol(
      final FrameHandler frameHandler,
      final Handler<Buffer> frameWriter
  ) {
    this.frameHandler = frameHandler;
    this.frameWriter = frameWriter;
  }

  private void setFields(Buffer buffer, int channelID, int frameType) {
    buffer.setInt(0, MAGIC_NUMBER)
        .setInt(4, VERSION)
        .setInt(8, channelID)
        .setInt(12, frameType);
  }

  public void writeMessageFrame(int channelID, JsonObject message) {
    Buffer payload = message.toBuffer();
    Buffer buff = Buffer.buffer(16 + payload.length());
    setFields(buff, channelID, FRAME_TYPE_MSG);
    buff.setBuffer(16, payload);
    frameWriter.handle(buff);
  }

  public void writeDataFrame(int channelID, Buffer payload) {
    Buffer buff = Buffer.buffer(16 + payload.length());
    setFields(buff, channelID, FRAME_TYPE_DAT);
    buff.setBuffer(16, payload);
    frameWriter.handle(buff);
  }

  public void writeFlowFrame(int channelID, int bytes) {
    Buffer buff = Buffer.buffer(20);
    setFields(buff, channelID, FRAME_TYPE_FLO);
    buff.setInt(16, bytes);
    frameWriter.handle(buff);
  }

  public void writeCloseFrame(int channelID) {
    Buffer buff = Buffer.buffer(16);
    setFields(buff, channelID, FRAME_TYPE_CLS);
    frameWriter.handle(buff);
  }

  public void writeRequestFrame(int channelID, short requestType, JsonObject message) {
    Buffer payload = message.toBuffer();
    Buffer buff = Buffer.buffer(18 + payload.length());
    setFields(buff, channelID, FRAME_TYPE_REQ);
    buff.setShort(16, requestType)
        .setBuffer(18, payload);
    frameWriter.handle(buff);
  }

  public void handleBuffer(Buffer buffer) {
    if (buffer.length() < 16) {
      throw new IllegalStateException("Not enough data");
    }
    int magic = buffer.getInt(0);
    if (magic != MAGIC_NUMBER) {
      // TODO close connection and log
      throw new IllegalStateException("Not a ksql connection");
    }
    int version = buffer.getInt(4);
    if (version != VERSION) {
      // TODO version checking
      throw new IllegalStateException("Invalid version");
    }
    int channelID = buffer.getInt(8);
    int frameType = buffer.getInt(12);
    switch (frameType) {
      case FRAME_TYPE_MSG: {
        frameHandler.handleMessageFrame(channelID, buffer.slice(16, buffer.length()));
        break;
      }
      case FRAME_TYPE_DAT: {
        frameHandler.handleDataFrame(channelID, buffer.slice(16, buffer.length()));
        break;
      }
      case FRAME_TYPE_FLO: {
        int bytes = buffer.getInt(16);
        frameHandler.handleFlowFrame(channelID, bytes);
        break;
      }
      case FRAME_TYPE_CLS: {
        frameHandler.handleCloseFrame(channelID);
        break;
      }
      case FRAME_TYPE_REQ: {
        short requestType = buffer.getShort(16);
        frameHandler.handleRequestFrame(channelID, requestType, buffer.slice(18, buffer.length()));
        break;
      }
      default:
        throw new IllegalStateException("Invalid frame type " + frameType);
    }
  }

  public static void main(String[] args) {
    System.out.println(toInt('k', 's', 'q', 'l'));
  }

  private static int toInt(char c1, char c2, char c3, char c4) {
    return (((byte) c1) << 24) + (((byte) c2) << 16) + (((byte) c3) << 8) + ((byte) c4);
  }

}
