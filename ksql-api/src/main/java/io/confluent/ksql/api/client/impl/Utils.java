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

import io.confluent.ksql.api.client.KsqlDBClientException;
import io.vertx.core.json.JsonObject;
import java.util.concurrent.CompletableFuture;

public class Utils {

  static boolean checkReplyStatus(CompletableFuture<?> future, JsonObject reply) {
    String status = reply.getString("status");
    if (status == null) {
      throw new IllegalStateException("No status in reply");
    }
    if ("ok".equals(status)) {
      return true;
    } else if ("err".equals(status)) {
      String errMessage = reply.getString("err-msg");
      if (errMessage == null) {
        throw new IllegalStateException("No err-msg in err reply");
      }
      future.completeExceptionally(new KsqlDBClientException(errMessage));
      return false;
    } else {
      throw new IllegalStateException("Invalid status " + status);
    }
  }

}
