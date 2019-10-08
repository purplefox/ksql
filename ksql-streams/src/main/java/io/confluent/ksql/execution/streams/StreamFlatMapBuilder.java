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

package io.confluent.ksql.execution.streams;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.function.udtf.KudtfFlatMapper;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.util.List;
import org.apache.kafka.streams.kstream.KStream;

public final class StreamFlatMapBuilder {

  private StreamFlatMapBuilder() {
  }

  public static <K> KStream<K, GenericRow> build(
      final KStream<K, GenericRow> source,
      final List<FunctionCall> functionCalls,
      final FunctionRegistry functionRegistry,
      final LogicalSchema inputSchema,
      final LogicalSchema outputSchema) {


    final KudtfFlatMapper flatMapper = new KudtfFlatMapper(functionCalls, inputSchema,
        outputSchema, functionRegistry);

    return source.flatMapValues(flatMapper);
  }

}