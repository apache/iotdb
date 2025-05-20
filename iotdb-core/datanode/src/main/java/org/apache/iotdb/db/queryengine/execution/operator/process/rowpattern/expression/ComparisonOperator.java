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
      return ((Comparable<Object>) left).compareTo(right) < 0;
    }
  },
  //  GREATER_THAN {
  //    @Override
  //    public Object apply(Object left, Object right) {
  //      if (left == null || right == null) return false;
  //      System.out.println("left: " + left + ", right: " + right);
  //      return ((Comparable<Object>) left).compareTo(right) > 0;
  //    }
  //  },
  GREATER_THAN {
    @Override
    public Object apply(Object left, Object right) {
      if (left == null || right == null) {
        return false;
      }
      Comparable<?> leftComparable = castComparable(left);
      Comparable<?> rightComparable = castComparable(right);

      System.out.println("left: " + leftComparable + ", right: " + rightComparable);

      //noinspection unchecked
      return ((Comparable<Object>) leftComparable).compareTo(rightComparable) > 0;
    }
  },
  EQUAL {
    @Override
    public Object apply(Object left, Object right) {
      if (left == null && right == null) return true;
      if (left == null || right == null) return false;
      return ((Comparable<Object>) left).compareTo(right) == 0;
    }
  },
  NOT_EQUAL {
    @Override
    public Object apply(Object left, Object right) {
      // 可直接取反 EQUAL 的结果
      Boolean eq = (Boolean) EQUAL.apply(left, right);
      return !eq;
    }
  },
  //  LESS_THAN_OR_EQUAL {
  //    @Override
  //    public Object apply(Object left, Object right) {
  //      if (left == null || right == null) return false;
  //      return ((Comparable<Object>) left).compareTo(right) <= 0;
  //    }
  //  },
  LESS_THAN_OR_EQUAL {
    @Override
    public Object apply(Object left, Object right) {
      // 1. null 安全检查
      if (left == null || right == null) {
        return false;
      }
      // 2. 统一类型转换
      Comparable<?> leftComparable = castComparable(left);
      Comparable<?> rightComparable = castComparable(right);

      // 3. 打印调试（可选）
      System.out.println("left: " + leftComparable + ", right: " + rightComparable);

      // 4. 比较并返回结果
      //noinspection unchecked
      return ((Comparable<Object>) leftComparable).compareTo(rightComparable) <= 0;
    }
  },
  GREATER_THAN_OR_EQUAL {
    @Override
    public Object apply(Object left, Object right) {
      if (left == null || right == null) return false;
      return ((Comparable<Object>) left).compareTo(right) >= 0;
    }
  },
  IS_DISTINCT_FROM {
    @Override
    public Object apply(Object left, Object right) {
      // IS_DISTINCT_FROM 是 NOT_EQUAL 的同义词，可以直接复用 NOT_EQUAL 的逻辑
      return NOT_EQUAL.apply(left, right);
    }
  };

  /** 如果 obj 是 String，就尝试 parse 成 Double，否则原样返回。 */
  @SuppressWarnings("unchecked")
  private static Comparable<?> castComparable(Object obj) {
    if (obj instanceof String) {
      String s = (String) obj;
      try {
        return Double.valueOf(s);
      } catch (NumberFormatException e) {
        return s; // 不是数字，就保留字符串
      }
    }
    if (obj instanceof Double) {
      return (Double) obj; // 已经是 Double，直接用
    }
    if (obj instanceof Number) {
      return ((Number) obj).doubleValue(); // 其他数字类型，转成 double
    }
    if (obj instanceof Comparable) {
      return (Comparable<?>) obj;
    }
    throw new IllegalArgumentException("Cannot compare object: " + obj);
  }
}
