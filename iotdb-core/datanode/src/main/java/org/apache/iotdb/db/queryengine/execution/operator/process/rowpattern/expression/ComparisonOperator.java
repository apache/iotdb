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
  GREATER_THAN {
    @Override
    public Object apply(Object left, Object right) {
      if (left == null || right == null) return false;
      return ((Comparable<Object>) left).compareTo(right) > 0;
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
  LESS_THAN_OR_EQUAL {
    @Override
    public Object apply(Object left, Object right) {
      if (left == null || right == null) return false;
      return ((Comparable<Object>) left).compareTo(right) <= 0;
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
}
