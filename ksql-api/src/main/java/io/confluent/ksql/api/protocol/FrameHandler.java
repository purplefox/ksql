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

import io.confluent.ksql.api.protocol.ProtocolHandler.AckFrame;
import io.confluent.ksql.api.protocol.ProtocolHandler.CloseFrame;
import io.confluent.ksql.api.protocol.ProtocolHandler.DataFrame;
import io.confluent.ksql.api.protocol.ProtocolHandler.FlowFrame;
import io.confluent.ksql.api.protocol.ProtocolHandler.MessageFrame;

public interface FrameHandler {

  void handleMessageFrame(MessageFrame messageFrame);

  void handleDataFrame(DataFrame dataFrame);

  void handleAckFrame(AckFrame ackFrame);

  void handleFlowFrame(FlowFrame flowFrame);

  void handleCloseFrame(CloseFrame closeFrame);

}
