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
package org.apache.iotdb.db.qp.logical.sys;

import org.apache.iotdb.db.exception.query.LogicalOperatorException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.CountPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowPlan.ShowContentType;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;

/** CountOperator is used to count time-series and count nodes. */
public class CountOperator extends ShowOperator {
  private PartialPath path;
  private int level;

  public CountOperator(int tokenIntType, PartialPath path) {
    super(tokenIntType);
    this.path = path;
  }

  public CountOperator(int tokenIntType, PartialPath path, int level) {
    super(tokenIntType);
    this.path = path;
    this.level = level;
  }

  public PartialPath getPath() {
    return this.path;
  }

  public int getLevel() {
    return this.level;
  }

  @Override
  public PhysicalPlan generatePhysicalPlan(PhysicalGenerator generator)
      throws QueryProcessException {
    switch (tokenIntType) {
      case SQLConstant.TOK_COUNT_DEVICES:
        return new CountPlan(ShowContentType.COUNT_DEVICES, path);
      case SQLConstant.TOK_COUNT_STORAGE_GROUP:
        return new CountPlan(ShowContentType.COUNT_STORAGE_GROUP, path);
      case SQLConstant.TOK_COUNT_NODE_TIMESERIES:
        return new CountPlan(ShowContentType.COUNT_NODE_TIMESERIES, path, level);
      case SQLConstant.TOK_COUNT_NODES:
        return new CountPlan(ShowContentType.COUNT_NODES, path, level);
      case SQLConstant.TOK_COUNT_TIMESERIES:
        return new CountPlan(ShowContentType.COUNT_TIMESERIES, path);
      default:
        throw new LogicalOperatorException(
            String.format("not supported operator type %s in show operation.", operatorType));
    }
  }
}
