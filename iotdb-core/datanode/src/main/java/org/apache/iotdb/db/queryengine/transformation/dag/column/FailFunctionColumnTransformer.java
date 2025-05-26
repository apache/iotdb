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

package org.apache.iotdb.db.queryengine.transformation.dag.column;

import org.apache.iotdb.db.exception.sql.SemanticException;

import org.apache.tsfile.read.common.block.column.NullColumn;
import org.apache.tsfile.read.common.type.Type;

/**
 * when evaluate of FailFunctionColumnTransformer is called, throw Exception to inform the user with
 * specified errorMsg
 */
public class FailFunctionColumnTransformer extends ColumnTransformer {
  public static final String FAIL_FUNCTION_NAME = "fail";

  private final String errorMsg;

  public FailFunctionColumnTransformer(Type returnType, String errorMsg) {
    super(returnType);
    this.errorMsg = errorMsg;
  }

  @Override
  protected void evaluate() {
    throw new SemanticException(errorMsg);
  }

  @Override
  public void evaluateWithSelection(boolean[] selection) {
    // if there is true value in selection, throw exception
    for (boolean b : selection) {
      if (b) {
        throw new SemanticException(errorMsg);
      }
    }
    // Result of fail function should be ignored, we just fake the output here.
    initializeColumnCache(new NullColumn(1));
  }

  @Override
  protected void checkType() {
    // do nothing
  }
}
