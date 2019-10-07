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

package io.confluent.ksql.execution.function.udtf;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.function.UdtfUtil;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.KsqlTableFunction;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.ColumnRef;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;
import org.apache.kafka.streams.kstream.ValueMapper;

public class KudtfFlatMapper implements ValueMapper<GenericRow, Iterable<GenericRow>> {

  private final List<FunctionCall> udtfCalls;
  private final LogicalSchema inputSchema;
  private final LogicalSchema outputSchema;
  private final FunctionRegistry functionRegistry;

  public KudtfFlatMapper(
      final List<FunctionCall> udtfCalls,
      final LogicalSchema inputSchema,
      final LogicalSchema outputSchema,
      final FunctionRegistry functionRegistry
  ) {
    this.udtfCalls = udtfCalls;
    this.inputSchema = inputSchema;
    this.outputSchema = outputSchema;
    this.functionRegistry = functionRegistry;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Iterable<GenericRow> apply(final GenericRow row) {

    // Just one udtf for now
    final FunctionCall functionCall = udtfCalls.get(0);
    final ColumnReferenceExp exp = (ColumnReferenceExp)functionCall.getArguments().get(0);
    final ColumnRef columnRef = exp.getReference();
    final ColumnName columnName = columnRef.name();
    final OptionalInt indexInInput = inputSchema.valueColumnIndex("TEST." + columnName.name());
    if (!indexInInput.isPresent()) {
      throw new IllegalArgumentException("Can't find input column " + columnName);
    }

    final ColumnName outputColumnName = ColumnName.udtfColumn(0);
    final OptionalInt outputIndex = outputSchema.valueColumnIndex(outputColumnName);
    if (!outputIndex.isPresent()) {
      throw new IllegalArgumentException("Can't find output column " + outputColumnName);
    }

    final List<Object> unexplodedValue = (List<Object>)row.getColumnValue(indexInInput.getAsInt());

    final KsqlTableFunction tableFunction = UdtfUtil.resolveTableFunction(
        functionRegistry,
        functionCall,
        inputSchema
    );

    final List<Object> list = tableFunction.flatMap(unexplodedValue);

    final List<GenericRow> rows = new ArrayList<>();
    for (Object val : list) {
//      final GenericRow gr =
//          new GenericRow(Arrays.asList(
//              row.getColumnValue(0), //rowtime
//              row.getColumnValue(1), //rowkey
//              row.getColumnValue(2), // id
//              row.getColumnValue(3), // original array before explode
//              val // val
//          ));
      final GenericRow gr = new GenericRow(new ArrayList<>(row.getColumns()));
      gr.getColumns().set(outputIndex.getAsInt(), val);
      rows.add(gr);
    }

    return rows;
  }
}
