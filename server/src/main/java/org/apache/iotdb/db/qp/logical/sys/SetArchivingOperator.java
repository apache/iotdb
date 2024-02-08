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

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.exception.runtime.SQLParserException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.SetArchivingPlan;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;

import java.io.File;

public class SetArchivingOperator extends Operator {
  private PartialPath storageGroup = null;
  private File targetDir = null;
  private Long ttl = null;
  private Long startTime = null;

  public SetArchivingOperator(int tokenIntType) {
    super(tokenIntType);
    this.operatorType = OperatorType.SET_ARCHIVING;
  }

  public PartialPath getStorageGroup() {
    return storageGroup;
  }

  public void setStorageGroup(PartialPath storageGroup) {
    this.storageGroup = storageGroup;
  }

  public File getTargetDir() {
    return targetDir;
  }

  public void setTargetDir(File targetDir) {
    this.targetDir = targetDir;
  }

  public long getTTL() {
    return ttl;
  }

  public void setTTL(long ttl) {
    this.ttl = ttl;
  }

  public long getStartTime() {
    return startTime;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  @Override
  public PhysicalPlan generatePhysicalPlan(PhysicalGenerator generator)
      throws QueryProcessException {
    if (storageGroup == null) {
      throw new SQLParserException("storage_group not specified");
    }
    if (startTime == null) {
      throw new SQLParserException("start_time not specified");
    }
    if (ttl == null) {
      throw new SQLParserException("ttl not specified");
    }
    if (targetDir == null) {
      throw new SQLParserException("target_dir not specified");
    }

    return new SetArchivingPlan(storageGroup, targetDir, ttl, startTime);
  }
}
