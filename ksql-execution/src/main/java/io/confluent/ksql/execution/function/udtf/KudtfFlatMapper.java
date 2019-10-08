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

    // TODO this is a hack use CodeGenRunner to evaluate the expressions that the udtfs work on
    // Just one udtf for now
    final FunctionCall functionCall = udtfCalls.get(0);
    final ColumnReferenceExp exp = (ColumnReferenceExp) functionCall.getArguments().get(0);
    final ColumnRef columnRef = exp.getReference();
    final ColumnName columnName = columnRef.name();

    final ColumnRef ref = ColumnRef.withoutSource(ColumnName.of(columnName.name()));

    final OptionalInt indexInInput = inputSchema.valueColumnIndex(ref);
    if (!indexInInput.isPresent()) {
      throw new IllegalArgumentException("Can't find input column " + columnName);
    }

    final ColumnName outputColumnName = ColumnName.udtfColumn(0);
    final ColumnRef refOutput = ColumnRef.withoutSource(outputColumnName);
    final OptionalInt outputIndex = outputSchema.valueColumnIndex(refOutput);
    if (!outputIndex.isPresent()) {
      throw new IllegalArgumentException("Can't find output column " + outputColumnName);
    }

    final List<Object> unexplodedValue = row.getColumnValue(indexInInput.getAsInt());

    final KsqlTableFunction tableFunction = UdtfUtil.resolveTableFunction(
        functionRegistry,
        functionCall,
        inputSchema
    );

    final List<Object> list = tableFunction.flatMap(unexplodedValue);

    final List<GenericRow> rows = new ArrayList<>();
    for (Object val : list) {
      final ArrayList<Object> arrayList = new ArrayList<>(row.getColumns());
      arrayList.add(val);
      rows.add(new GenericRow(arrayList));
    }

    return rows;
  }
}
