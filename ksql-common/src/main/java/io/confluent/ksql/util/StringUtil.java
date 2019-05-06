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

package io.confluent.ksql.util;

import java.util.List;

public final class StringUtil {

  private StringUtil() {
  }

  public static String cleanQuotes(final String stringWithQuotes) {
    // TODO: move check to grammar
    if (!stringWithQuotes.startsWith("'") || !stringWithQuotes.endsWith("'")) {
      return stringWithQuotes;
    }
    return stringWithQuotes
        .substring(1, stringWithQuotes.length() - 1)
        .replaceAll("''", "'");
  }

  public static String join(final String delimiter, final List<?> objs) {
    final StringBuilder sb = new StringBuilder();
    int cnt = 0;
    for (final Object obj : objs) {
      if (cnt > 0) {
        sb.append(delimiter);
      }
      sb.append(obj);
      cnt += 1;
    }
    return sb.toString();
  }

}
