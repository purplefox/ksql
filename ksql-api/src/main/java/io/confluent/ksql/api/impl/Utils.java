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

package io.confluent.ksql.api.impl;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class Utils {

  public static <T> CompletableFuture<T> convertFuture(Future<T> future) {
    CompletableFuture<T> jdkFuture = new CompletableFuture<>();
    future.setHandler(ar -> {
      if (ar.succeeded()) {
        jdkFuture.complete(ar.result());
      } else {
        jdkFuture.completeExceptionally(ar.cause());
      }
    });
    return jdkFuture;
  }

  public static CompletableFuture<Void> shutdownExecutorServiceAsync(
      ExecutorService executorService) {
    final Promise<Void> promise = Promise.promise();
    new Thread(() -> {
      executorService.shutdown();
      try {
        if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
          promise.fail(new TimeoutException("Timed out in shutting down executor service"));
        }
      } catch (Exception e) {
        promise.fail(e);
      }
    }).start();
    return Utils.convertFuture(promise.future());
  }

}
