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
package org.apache.iotdb.db.qp.logical.crud;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class presents series condition which is general(e.g. numerical comparison) or defined by
 * user. Function is used for bottom operator.<br>
 * FunctionOperator has a {@code seriesPath}, and other filter condition.
 */
public class FunctionOperator extends FilterOperator {

  private static final Logger logger = LoggerFactory.getLogger(FunctionOperator.class);

  public FunctionOperator(int tokenIntType) {
    super(tokenIntType);
    operatorType = OperatorType.FUNC;
  }

  /** reverse func. */
  public void reverseFunc() {
    // Implemented by subclass
  }

  @Override
  public boolean addChildOperator(FilterOperator op) {
    logger.error("cannot add child to leaf FilterOperator, now it's FunctionOperator");
    return false;
  }
}
