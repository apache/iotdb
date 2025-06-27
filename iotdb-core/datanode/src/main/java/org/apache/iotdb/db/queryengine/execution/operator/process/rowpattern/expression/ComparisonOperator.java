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

import org.apache.tsfile.utils.Binary;

import java.nio.charset.StandardCharsets;

public enum ComparisonOperator implements BinaryOperator {
  LESS_THAN {
    @Override
    public Object apply(Object left, Object right) {
      if (left == null || right == null) return false;
      return compare(left, right) < 0;
    }
  },
  GREATER_THAN {
    @Override
    public Object apply(Object left, Object right) {
      if (left == null || right == null) {
        return false;
      }
      return compare(left, right) > 0;
    }
  },
  EQUAL {
    @Override
    public Object apply(Object left, Object right) {
      if (left == null && right == null) return true;
      if (left == null || right == null) return false;
      return compare(left, right) == 0;
    }
  },
  NOT_EQUAL {
    @Override
    public Object apply(Object left, Object right) {
      Boolean eq = (Boolean) EQUAL.apply(left, right);
      return !eq;
    }
  },
  LESS_THAN_OR_EQUAL {
    @Override
    public Object apply(Object left, Object right) {
      if (left == null || right == null) {
        return false;
      }
      return compare(left, right) <= 0;
    }
  },
  GREATER_THAN_OR_EQUAL {
    @Override
    public Object apply(Object left, Object right) {
      if (left == null || right == null) return false;
      return compare(left, right) >= 0;
    }
  },
  IS_DISTINCT_FROM {
    @Override
    public Object apply(Object left, Object right) {
      return NOT_EQUAL.apply(left, right);
    }
  };

  private static int compare(Object left, Object right) {
    if (left == null || right == null) return -1; // null cases are left to the caller for handling

    NormalizedValue normLeft = normalize(left);
    NormalizedValue normRight = normalize(right);

    if (normLeft.type != normRight.type) {
      throw new SemanticException(
          "Cannot compare values of different types: " + normLeft.type + " vs. " + normRight.type);
    }

    return normLeft.compareTo(normRight);
  }

  /** provide a unified comparison logic */
  private static class NormalizedValue implements Comparable<NormalizedValue> {
    enum Type {
      DOUBLE,
      STRING,
      BOOLEAN,
      BINARY
    }

    final Type type;
    final Object value;

    NormalizedValue(Type type, Object value) {
      this.type = type;
      this.value = value;
    }

    @Override
    public int compareTo(NormalizedValue other) {
      switch (type) {
        case DOUBLE:
          return Double.compare((Double) value, (Double) other.value);
        case STRING:
          return ((String) value).compareTo((String) other.value);
        case BOOLEAN:
          return Boolean.compare((Boolean) value, (Boolean) other.value);
        case BINARY:
          return ((Binary) value).compareTo((Binary) other.value);
        default:
          throw new SemanticException("Unknown type: " + type);
      }
    }
  }

  /** type normalization logic */
  private static NormalizedValue normalize(Object obj) {
    if (obj instanceof String) {
      String s = (String) obj;
      Binary binary = new Binary(s, StandardCharsets.UTF_8);
      return new NormalizedValue(NormalizedValue.Type.BINARY, binary);
    } else if (obj instanceof Number) {
      return new NormalizedValue(NormalizedValue.Type.DOUBLE, ((Number) obj).doubleValue());
    } else if (obj instanceof Boolean) {
      return new NormalizedValue(NormalizedValue.Type.BOOLEAN, obj);
    } else if (obj instanceof Binary) {
      return new NormalizedValue(NormalizedValue.Type.BINARY, obj);
    } else {
      throw new SemanticException("Unsupported type: " + obj.getClass());
    }
  }
}
