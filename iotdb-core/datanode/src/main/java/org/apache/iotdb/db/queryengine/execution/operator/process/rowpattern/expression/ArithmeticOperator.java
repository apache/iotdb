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

package org.apache.iotdb.db.queryengine.execution.operator.process.rowpattern.expression;

import org.apache.iotdb.db.exception.sql.SemanticException;

public enum ArithmeticOperator implements BinaryOperator {
  ADD {
    @Override
    public Object apply(Object left, Object right) {
      if (left == null || right == null) return null;
      return normalizeToDouble(left) + normalizeToDouble(right);
    }
  },
  SUBTRACT {
    @Override
    public Object apply(Object left, Object right) {
      if (left == null || right == null) return null;
      return normalizeToDouble(left) - normalizeToDouble(right);
    }
  },
  MULTIPLY {
    @Override
    public Object apply(Object left, Object right) {
      if (left == null || right == null) return null;
      return ((Number) left).intValue() * ((Number) right).intValue();
    }
  },
  DIVIDE {
    @Override
    public Object apply(Object left, Object right) {
      if (left == null || right == null) return null;
      double r = normalizeToDouble(right);
      if (r == 0.0) {
        throw new ArithmeticException("Division by zero");
      }
      return normalizeToDouble(left) / r;
    }
  },
  MODULUS {
    @Override
    public Object apply(Object left, Object right) {
      if (left == null || right == null) return null;
      double r = normalizeToDouble(right);
      if (r == 0.0) {
        throw new ArithmeticException("Modulus by zero");
      }
      return normalizeToDouble(left) % r;
    }
  };

  private static double normalizeToDouble(Object obj) {
    if (obj instanceof Number) {
      return ((Number) obj).doubleValue();
    } else if (obj instanceof String) {
      try {
        return Double.parseDouble((String) obj);
      } catch (NumberFormatException e) {
        throw new SemanticException("Cannot parse String to double: " + obj);
      }
    } else {
      throw new SemanticException("Unsupported type for arithmetic operation: " + obj.getClass());
    }
  }
}
