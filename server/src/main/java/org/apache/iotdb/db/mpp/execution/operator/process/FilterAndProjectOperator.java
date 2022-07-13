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

package org.apache.iotdb.db.mpp.execution.operator.process;

import org.apache.iotdb.db.mpp.execution.operator.Operator;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.plan.analyze.TypeProvider;
import org.apache.iotdb.db.mpp.transformation.dag.udf.UDTFContext;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;

import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilterAndProjectOperator implements ProcessOperator {

  private static final Logger LOGGER = LoggerFactory.getLogger(FilterAndProjectOperator.class);

  private final Operator inputOperator;

  private final UDTFContext predicateUDTFContext;

  private final UDTFContext outputUDTFContext;
  private final OperatorContext operatorContext;

  public FilterAndProjectOperator(
      OperatorContext operatorContext, Operator inputOperator, TypeProvider typeProvider) {
    this.operatorContext = operatorContext;
    this.inputOperator = inputOperator;
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public TsBlock next() {
    return null;
  }

  @Override
  public boolean hasNext() {
    return inputOperator.hasNext();
  }

  @Override
  public boolean isFinished() {
    return inputOperator.isFinished();
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    return inputOperator.isBlocked();
  }
}
