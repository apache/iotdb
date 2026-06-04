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
package org.apache.iotdb.db.queryengine.execution.operator;

import org.apache.iotdb.calc.execution.operator.Operator;

import org.apache.tsfile.read.common.block.TsBlock;

public final class OperatorTestUtils {

  private OperatorTestUtils() {
    // Utility class.
  }

  public static TsBlock nextNonEmpty(Operator operator) throws Exception {
    TsBlock result = nextNonEmptyOrNull(operator);
    if (result != null) {
      return result;
    }
    throw new AssertionError("Expected a non-empty TsBlock from operator");
  }

  public static TsBlock nextNonEmptyOrNull(Operator operator) throws Exception {
    while (operator.hasNext()) {
      TsBlock result = operator.next();
      if (!isNullOrEmpty(result)) {
        return result;
      }
    }
    return null;
  }

  public static TsBlock lastNonEmpty(Operator operator) throws Exception {
    TsBlock result = null;
    while (operator.isBlocked().isDone() && operator.hasNext()) {
      TsBlock nextResult = operator.next();
      if (!isNullOrEmpty(nextResult)) {
        result = nextResult;
      }
    }
    return result;
  }

  private static boolean isNullOrEmpty(TsBlock tsBlock) {
    return tsBlock == null || tsBlock.getPositionCount() == 0;
  }
}
