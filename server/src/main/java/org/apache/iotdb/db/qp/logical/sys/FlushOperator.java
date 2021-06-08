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

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.FlushPlan;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;

import java.util.List;

public class FlushOperator extends Operator {

  public List<PartialPath> getStorageGroupList() {
    return storageGroupList;
  }

  public void setStorageGroupList(List<PartialPath> storageGroupList) {
    this.storageGroupList = storageGroupList;
  }

  private List<PartialPath> storageGroupList;

  public Boolean isSeq() {
    return isSeq;
  }

  public void setSeq(boolean seq) {
    isSeq = seq;
  }

  private Boolean isSeq;

  public FlushOperator(int tokenIntType) {
    super(tokenIntType);
    operatorType = OperatorType.FLUSH;
  }

  @Override
  public PhysicalPlan generatePhysicalPlan(PhysicalGenerator generator)
      throws QueryProcessException {
    return new FlushPlan(isSeq, storageGroupList);
  }
}
