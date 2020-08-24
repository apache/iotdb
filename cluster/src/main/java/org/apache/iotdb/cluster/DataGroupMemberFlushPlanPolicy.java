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

package org.apache.iotdb.cluster;

import java.util.List;
import org.apache.iotdb.cluster.server.member.DataGroupMember;
import org.apache.iotdb.db.engine.flush.TsFileFlushPolicy;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor;
import org.apache.iotdb.db.engine.storagegroup.TsFileProcessor;
import org.apache.iotdb.db.qp.physical.sys.FlushPlan;
import org.apache.iotdb.tsfile.read.common.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataGroupMemberFlushPlanPolicy implements TsFileFlushPolicy {

  private static final Logger logger = LoggerFactory
      .getLogger(DataGroupMemberFlushPlanPolicy.class);
  private DataGroupMember dataGroupMember;

  public DataGroupMemberFlushPlanPolicy(DataGroupMember dataGroupMember) {
    this.dataGroupMember = dataGroupMember;
  }

  @Override
  public void apply(StorageGroupProcessor storageGroupProcessor, TsFileProcessor processor,
      boolean isSeq) {
    // do nothing
  }

  @Override
  public void apply(List<Path> storeGroups) {
    FlushPlan plan = new FlushPlan(null, true, storeGroups);
    dataGroupMember.flushFileWhenDoSnapshot(plan);
  }
}

