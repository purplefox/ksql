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
import io.confluent.ksql.execution.function.UdtfUtil;
import io.confluent.ksql.execution.function.udtf.KudtfFlatMapper;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.KsqlTableFunction;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import org.apache.kafka.streams.kstream.KStream;

public final class StreamFlatMapBuilder {

  private StreamFlatMapBuilder() {
  }

  public static <K> KStream<K, GenericRow> build(
      final KStream<K, GenericRow> source,
      final FunctionCall functionCall,
      final FunctionRegistry functionRegistry,
      final LogicalSchema schema) {

    final KsqlTableFunction function = UdtfUtil.resolveTableFunction(
        functionRegistry,
        functionCall,
        schema
    );

    final KudtfFlatMapper flatMapper = new KudtfFlatMapper(function);

    return source.flatMapValues(flatMapper);
  }

}