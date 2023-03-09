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

package org.apache.iotdb.db.mpp.plan.expression.multi.builtin;

import org.apache.iotdb.db.mpp.plan.expression.multi.builtin.helper.CastHelper;
import org.apache.iotdb.db.mpp.plan.expression.multi.builtin.helper.DiffHelper;

public class BuiltInScalarFunctionHelperFactory {
  public static BuiltInScalarFunctionHelper createHelper(String functionName) {
    functionName = functionName.toUpperCase();
    switch (functionName) {
      case "DIFF":
        return new DiffHelper();
      case "CAST":
        return new CastHelper();
      default:
        throw new IllegalArgumentException(
            String.format("Invalid scalar function [%s].", functionName));
    }
  }
}
