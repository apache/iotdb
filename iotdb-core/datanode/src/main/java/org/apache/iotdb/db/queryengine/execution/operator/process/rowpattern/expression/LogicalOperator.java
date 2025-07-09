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

import java.util.List;

public enum LogicalOperator implements NaryOperator {
  AND {
    @Override
    public Object apply(List<Object> operands) {
      for (Object operand : operands) {
        if (!(operand instanceof Boolean)) {
          throw new IllegalArgumentException("AND operator only accepts Boolean operands");
        }
        if (!((Boolean) operand)) {
          return false; // short-circuit
        }
      }
      return true;
    }
  },
  OR {
    @Override
    public Object apply(List<Object> operands) {
      for (Object operand : operands) {
        if (!(operand instanceof Boolean)) {
          throw new SemanticException("OR operator only accepts Boolean operands");
        }
        if ((Boolean) operand) {
          return true; // short-circuit
        }
      }
      return false;
    }
  };
}
