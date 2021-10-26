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
package org.apache.iotdb.db.qp.physical.crud;

import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.query.executor.fill.IFill;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.Map;

public class FillQueryPlan extends RawDataQueryPlan {

  private long queryTime;
  private Map<TSDataType, IFill> fillType;

  public FillQueryPlan() {
    super();
    setOperatorType(Operator.OperatorType.FILL);
  }

  public long getQueryTime() {
    return queryTime;
  }

  public void setQueryTime(long queryTime) {
    this.queryTime = queryTime;
  }

  public Map<TSDataType, IFill> getFillType() {
    return fillType;
  }

  public void setFillType(Map<TSDataType, IFill> fillType) {
    this.fillType = fillType;
  }
}
