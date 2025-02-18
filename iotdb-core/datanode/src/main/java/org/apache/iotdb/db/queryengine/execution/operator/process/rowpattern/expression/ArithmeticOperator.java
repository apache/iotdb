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

// TODO: 目前只支持 int 类型运算，后续可根据需要扩展
public enum ArithmeticOperator implements BinaryOperator {
  ADD {
    @Override
    public Object apply(Object left, Object right) {
      if (left == null || right == null) return null;
      // 这里简单以 int 计算，实际可根据类型进行更精细的处理
      return ((Number) left).intValue() + ((Number) right).intValue();
    }
  },
  SUBTRACT {
    @Override
    public Object apply(Object left, Object right) {
      if (left == null || right == null) return null;
      return ((Number) left).intValue() - ((Number) right).intValue();
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
      int r = ((Number) right).intValue();
      if (r == 0) {
        throw new ArithmeticException("Division by zero");
      }
      return ((Number) left).intValue() / r;
    }
  },
  MODULUS {
    @Override
    public Object apply(Object left, Object right) {
      if (left == null || right == null) return null;
      int r = ((Number) right).intValue();
      if (r == 0) {
        throw new ArithmeticException("Modulus by zero");
      }
      return ((Number) left).intValue() % r;
    }
  };
}
