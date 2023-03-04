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

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import static org.apache.iotdb.db.conf.IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD;

/** this class extends {@code RootOperator} and process delete statement. */
public class DeleteDataOperator extends Operator {

  private final List<PartialPath> paths;

  private long startTime;
  private long endTime;

  public DeleteDataOperator(int tokenIntType) {
    super(tokenIntType);
    operatorType = Operator.OperatorType.DELETE;
    paths = new ArrayList<>();
  }

  public List<PartialPath> getPaths() {
    return paths;
  }

  public void addPath(PartialPath path) {
    paths.add(path);
  }

  public long getStartTime() {
    return startTime;
  }

  public void setStartTime(long time) {
    this.startTime = time;
  }

  public long getEndTime() {
    return endTime;
  }

  public void setEndTime(long time) {
    this.endTime = time;
  }

  @Override
  public PhysicalPlan generatePhysicalPlan(PhysicalGenerator generator)
      throws QueryProcessException {
    List<PartialPath> originPath = getPaths();
    if (isPrefixMatchPath()) {
      // adapt to prefix match of 0.12
      List<PartialPath> addedPath = new LinkedList<>();
      for (PartialPath path : originPath) {
        addedPath.add(path.concatNode(MULTI_LEVEL_PATH_WILDCARD));
      }
      originPath.addAll(addedPath);
    }
    return new DeletePlan(getStartTime(), getEndTime(), originPath);
  }
}
