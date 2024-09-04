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

package org.apache.iotdb.db.queryengine.plan.relational.function;

import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.TableBuiltinScalarFunction;

import org.apache.tsfile.read.common.type.Type;

import java.util.List;

public class InterpretedFunctionInvoker {

  /**
   * Arguments must be the native container type for the corresponding SQL types.
   *
   * <p>Returns a value in the native container type corresponding to the declared SQL return type
   */
  public Object invoke(
      TableBuiltinScalarFunction function,
      SessionInfo session,
      List<? extends Type> argumentTypes,
      List<Object> arguments) {
    throw new UnsupportedOperationException(
        String.format("BuiltinScalarFunctio %s cannot be invoked.", function));
  }

  public Object invoke(
      OperatorType operatorType,
      SessionInfo session,
      List<? extends Type> argumentTypes,
      List<Object> arguments) {
    throw new UnsupportedOperationException(
        String.format("OperatorType %s cannot be invoked.", operatorType));
    //    switch (operatorType) {
    //      case ADD:
    //        Object left = arguments.get(0);
    //        Object right = arguments.get(1);
    //        if (left == null || right == null) {
    //          return null;
    //        }
    //
    //        break;
    //      case SUBTRACT:
    //        break;
    //      case MULTIPLY:
    //        break;
    //      case DIVIDE:
    //        break;
    //      case MODULUS:
    //        break;
    //      case NEGATION:
    //        Object value = arguments.get(0);
    //        break;
    //      case EQUAL:
    //        break;
    //      case LESS_THAN:
    //        break;
    //      case LESS_THAN_OR_EQUAL:
    //        break;
    //      default:
    //        throw new UnsupportedOperationException(String.format("OperatorType %s cannot be
    // invoked.", operatorType));
    //    }
  }
}
