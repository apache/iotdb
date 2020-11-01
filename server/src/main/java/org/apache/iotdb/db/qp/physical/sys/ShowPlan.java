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
package org.apache.iotdb.db.qp.physical.sys;

import java.util.Collections;
import java.util.List;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;

public class ShowPlan extends PhysicalPlan {

  private ShowContentType showContentType;

  public ShowPlan(ShowContentType showContentType){
    super(true);
    this.showContentType = showContentType;
    setOperatorType(OperatorType.SHOW);
  }

  @Override
  public List<PartialPath> getPaths() {
    return Collections.emptyList();
  }

  public ShowContentType getShowContentType() {
    return showContentType;
  }

  @Override
  public String toString() {
    return String.format("%s %s", getOperatorType().toString(), showContentType);
  }

  public enum ShowContentType {
    DYNAMIC_PARAMETER, FLUSH_TASK_INFO, TTL, VERSION, TIMESERIES, STORAGE_GROUP, CHILD_PATH, DEVICES,
    COUNT_TIMESERIES, COUNT_NODE_TIMESERIES, COUNT_NODES, MERGE_STATUS, COUNT_DEVICES, COUNT_STORAGE_GROUP
  }

}
