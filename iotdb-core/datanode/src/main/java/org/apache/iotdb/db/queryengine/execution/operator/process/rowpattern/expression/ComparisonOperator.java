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

  /** 如果 obj 是 String，就尝试 parse 成 Double，否则原样返回。 */
  //  @SuppressWarnings("unchecked")
  //  private static Comparable<?> castComparable(Object obj) {
  //    if (obj instanceof String) {
  //      String s = (String) obj;
  //      try {
  //        return Double.valueOf(s);
  //      } catch (NumberFormatException e) {
  //        return s; // 不是数字，就保留字符串
  //      }
  //    }
  //    if (obj instanceof Double) {
  //      return (Double) obj; // 已经是 Double，直接用
  //    }
  //    if (obj instanceof Number) {
  //      return ((Number) obj).doubleValue(); // 其他数字类型，转成 double
  //    }
  //    if (obj instanceof Comparable) {
  //      return (Comparable<?>) obj;
  //    }
  //    throw new IllegalArgumentException("Cannot compare object: " + obj);
  //  }

  private static int compare(Object left, Object right) {
    if (left == null || right == null) return -1; // null cases are left to the caller for handling

    NormalizedValue normLeft = normalize(left);
    NormalizedValue normRight = normalize(right);

    if (normLeft.type != normRight.type) {
      throw new IllegalArgumentException(
          "Cannot compare values of different types: " + normLeft.type + " vs. " + normRight.type);
    }

    return normLeft.compareTo(normRight);
  }

  /** provide a unified comparison logic */
  private static class NormalizedValue implements Comparable<NormalizedValue> {
    enum Type {
      DOUBLE,
      STRING,
      BOOLEAN
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
        default:
          throw new IllegalStateException("Unknown type: " + type);
      }
    }
  }

  /** type normalization logic */
  private static NormalizedValue normalize(Object obj) {
    if (obj instanceof String) {
      String s = (String) obj;
      try {
        return new NormalizedValue(NormalizedValue.Type.DOUBLE, Double.valueOf(s));
      } catch (NumberFormatException e) {
        return new NormalizedValue(NormalizedValue.Type.STRING, s);
      }
    } else if (obj instanceof Number) {
      return new NormalizedValue(NormalizedValue.Type.DOUBLE, ((Number) obj).doubleValue());
    } else if (obj instanceof Boolean) {
      return new NormalizedValue(NormalizedValue.Type.BOOLEAN, obj);
    } else {
      throw new IllegalArgumentException("Unsupported type: " + obj.getClass());
    }
  }
}
