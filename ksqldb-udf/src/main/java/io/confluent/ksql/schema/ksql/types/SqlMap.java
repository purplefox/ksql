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

package io.confluent.ksql.schema.ksql.types;

import static java.util.Objects.requireNonNull;

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.schema.ksql.JavaToSqlTypeConverter;
import io.confluent.ksql.schema.utils.DataException;
import io.confluent.ksql.schema.utils.FormatOptions;
import java.util.Map;
import java.util.Objects;

@Immutable
public final class SqlMap extends SqlType {

  private static final SqlType KEY_TYPE = SqlTypes.STRING;

  private final SqlType valueType;

  public static SqlMap of(final SqlType valueType) {
    return new SqlMap(valueType);
  }

  private SqlMap(final SqlType valueType) {
    super(SqlBaseType.MAP);
    this.valueType = requireNonNull(valueType, "valueType");
  }

  public SqlType getValueType() {
    return valueType;
  }

  @Override
  public void validateValue(final Object value) {
    if (value == null) {
      return;
    }

    if (!(value instanceof Map)) {
      final SqlBaseType sqlBaseType = JavaToSqlTypeConverter.instance()
          .toSqlType(value.getClass());

      throw new DataException("Expected MAP, got " + sqlBaseType);
    }

    final Map<?, ?> map = (Map<?, ?>) value;

    map.forEach((k, v) -> {
      try {
        KEY_TYPE.validateValue(k);
      } catch (final DataException e) {
        throw new DataException("MAP key: " + e.getMessage(), e);
      }

      try {
        valueType.validateValue(v);
      } catch (final DataException e) {
        throw new DataException("MAP value for key '" + k + "': " + e.getMessage(), e);
      }
    });
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final SqlMap map = (SqlMap) o;
    return Objects.equals(valueType, map.valueType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(valueType);
  }

  @Override
  public String toString() {
    return toString(FormatOptions.none());
  }

  @Override
  public String toString(final FormatOptions formatOptions) {
    return "MAP<" + SqlTypes.STRING + ", " + valueType.toString(formatOptions) + '>';
  }
}
