/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.function.udtf.array;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.function.KsqlTableFunction;
import io.confluent.ksql.function.TableFunctionFactory;
import io.confluent.ksql.util.KsqlException;
import java.util.List;
import org.apache.kafka.connect.data.Schema;

public class ExplodeIntegerArrayFunctionFactory extends TableFunctionFactory {

  private static final String NAME = "EXPLODE";

  private static final List<List<Schema>> SUPPORTED_TYPES = ImmutableList
      .<List<Schema>>builder()
      .add(ImmutableList.of(Schema.OPTIONAL_INT32_SCHEMA))
      .build();

  public ExplodeIntegerArrayFunctionFactory() {
    super(NAME);
  }

  @SuppressWarnings("unchecked")
  @Override
  public KsqlTableFunction getProperTableFunction(final List<Schema> argTypeList) {
    if (argTypeList.isEmpty()) {
      throw new KsqlException("EXPLODE function should have two arguments.");
    }

    return new ExplodeIntegerArrayUdtf(NAME, argTypeList.get(0), argTypeList, "Explodes an array");

  }

  @Override
  public List<List<Schema>> supportedArgs() {
    return SUPPORTED_TYPES;
  }
}