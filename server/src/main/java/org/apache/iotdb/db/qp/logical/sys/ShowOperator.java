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
 *
 */
package org.apache.iotdb.db.qp.logical.sys;

import org.apache.iotdb.db.exception.query.LogicalOperatorException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowContinuousQueriesPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowPlan.ShowContentType;
import org.apache.iotdb.db.qp.physical.sys.ShowQueryProcesslistPlan;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;

public class ShowOperator extends Operator {

  public ShowOperator(int tokenIntType) {
    this(tokenIntType, OperatorType.SHOW);
  }

  public ShowOperator(int tokenIntType, OperatorType operatorType) {
    super(tokenIntType);
    this.operatorType = operatorType;
  }

  @Override
  public PhysicalPlan generatePhysicalPlan(PhysicalGenerator generator)
      throws QueryProcessException {
    switch (tokenIntType) {
      case SQLConstant.TOK_FLUSH_TASK_INFO:
        return new ShowPlan(ShowContentType.FLUSH_TASK_INFO);
      case SQLConstant.TOK_VERSION:
        return new ShowPlan(ShowContentType.VERSION);
      case SQLConstant.TOK_QUERY_PROCESSLIST:
        return new ShowQueryProcesslistPlan(ShowContentType.QUERY_PROCESSLIST);
      case SQLConstant.TOK_SHOW_CONTINUOUS_QUERIES:
        return new ShowContinuousQueriesPlan();
      default:
        throw new LogicalOperatorException(
            String.format("not supported operator type %s in show operation.", operatorType));
    }
  }
}
