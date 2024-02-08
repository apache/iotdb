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
package org.apache.iotdb.db.qp.physical.sys;

import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;

import java.util.List;

public class PauseArchivingPlan extends PhysicalPlan {
  private long taskId = -1;
  private PartialPath storageGroup;
  private boolean pause = true;

  public PauseArchivingPlan(boolean pause) {
    super(Operator.OperatorType.PAUSE_ARCHIVING);
    this.pause = pause;
  }

  public PauseArchivingPlan(PartialPath storageGroup, boolean pause) {
    super(Operator.OperatorType.PAUSE_ARCHIVING);
    this.storageGroup = storageGroup;
    this.pause = pause;
  }

  public PauseArchivingPlan(long taskId, boolean pause) {
    super(Operator.OperatorType.PAUSE_ARCHIVING);
    this.taskId = taskId;
    this.pause = pause;
  }

  @Override
  public List<PartialPath> getPaths() {
    return null;
  }

  public long getTaskId() {
    return taskId;
  }

  public boolean isPause() {
    return pause;
  }

  public PartialPath getStorageGroup() {
    return storageGroup;
  }
}
