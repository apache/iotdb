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
import org.apache.iotdb.db.qp.logical.RootOperator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowPlan.ShowContentType;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;

public class ShowOperator extends RootOperator {

  public ShowOperator(int tokenIntType) {
    this(tokenIntType, OperatorType.SHOW);
  }

  @Override
  public PhysicalPlan transform2PhysicalPlan(int fetchSize,
      PhysicalGenerator generator) throws QueryProcessException {
    ShowContentType contentType = ShowContentType.getFromOperatorType(tokenIntType);
    if (contentType == null) {
      throw new LogicalOperatorException(String.format(
          "not supported operator type %s in show operation.", getType()));
    }

    return new ShowPlan(contentType);
  }

  public ShowOperator(int tokenIntType, OperatorType operatorType) {
    super(tokenIntType);
    this.operatorType = operatorType;
  }
}
