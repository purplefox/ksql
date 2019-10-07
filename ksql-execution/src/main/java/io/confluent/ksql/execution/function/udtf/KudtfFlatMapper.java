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
import io.confluent.ksql.function.KsqlTableFunction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.kafka.streams.kstream.ValueMapper;

public class KudtfFlatMapper implements ValueMapper<GenericRow, Iterable<GenericRow>> {

  private final KsqlTableFunction tableFunction;


  public KudtfFlatMapper(
      final KsqlTableFunction<?, ?> function
  ) {
    this.tableFunction = function;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Iterable<GenericRow> apply(final GenericRow row) {

    // TODO proper implementation
    // UDTF is always on 3rd column for now
    final List<Object> col0Value = (List<Object>)row.getColumnValue(3);
    final List<Object> list = tableFunction.flatMap(col0Value);
    final List<GenericRow> rows = new ArrayList<>();
    for (Object val : list) {
      final GenericRow gr =
          new GenericRow(Arrays.asList(
              21,
              22,
              row.getColumnValue(2), // id
              24,
              val // val
          ));
      rows.add(gr);
    }

    // For some reason:
    // col 0: key
    // col 1

    //return new ArrayList<>(Arrays.asList(new GenericRow(1, 2, 3, 4, 5)));
    return rows;
  }
}
