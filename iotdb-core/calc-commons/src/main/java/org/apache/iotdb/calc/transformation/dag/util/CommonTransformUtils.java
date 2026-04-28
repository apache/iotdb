/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.calc.transformation.dag.util;

import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.StringLiteral;

import org.apache.tsfile.utils.Binary;

import java.util.Objects;
import java.util.Optional;

public class CommonTransformUtils {
  public static int compare(Binary first, Binary second) {
    if (Objects.requireNonNull(first) == Objects.requireNonNull(second)) {
      return 0;
    }

    return first.compareTo(second);
  }

  public static Optional<Character> getEscapeCharacter(String escape) {
    if (escape.length() == 1) {
      return Optional.of(escape.charAt(0));
    } else {
      throw new SemanticException("Escape string must be a single character");
    }
  }

  public static boolean isStringLiteral(final Expression expression) {
    return expression instanceof StringLiteral;
  }
}
