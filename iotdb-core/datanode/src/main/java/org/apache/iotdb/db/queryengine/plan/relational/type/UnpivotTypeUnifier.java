/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.relational.type;

import org.apache.tsfile.read.common.type.Type;

import java.util.Optional;

import static org.apache.tsfile.read.common.type.BinaryType.TEXT;
import static org.apache.tsfile.read.common.type.DoubleType.DOUBLE;
import static org.apache.tsfile.read.common.type.FloatType.FLOAT;
import static org.apache.tsfile.read.common.type.IntType.INT32;
import static org.apache.tsfile.read.common.type.LongType.INT64;
import static org.apache.tsfile.read.common.type.StringType.STRING;

/** UNPIVOT permits a smaller set of coercions than the general expression resolver. */
public final class UnpivotTypeUnifier {
  private UnpivotTypeUnifier() {}

  public static Optional<Type> getCommonType(Type left, Type right) {
    if (left.equals(right)) {
      return Optional.of(left);
    }
    if ((left == TEXT && right == STRING) || (left == STRING && right == TEXT)) {
      return CompatibleResolver.getCommonSuperType(left, right);
    }
    if (isNumeric(left) && isNumeric(right)) {
      return CompatibleResolver.getCommonSuperType(left, right);
    }
    return Optional.empty();
  }

  private static boolean isNumeric(Type type) {
    return type == INT32 || type == INT64 || type == FLOAT || type == DOUBLE;
  }
}
