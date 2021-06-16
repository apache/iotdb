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

package org.apache.iotdb.db.engine.compaction.task;

import org.apache.iotdb.db.engine.storagegroup.TsFileResourceList;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceListNode;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class InnerSpaceCompactionTask extends AbstractCompactionTask {
  private List<TsFileResourceListNode> tsFileResourceList;
  private boolean sequence;
  private String storageGroup;

  public InnerSpaceCompactionTask(
      List<TsFileResourceListNode> tsFileResourceList,
      boolean sequence,
      String storageGroup,
      AtomicInteger globalActiveTaskNum) {
    super(globalActiveTaskNum);
    this.tsFileResourceList = tsFileResourceList;
    this.sequence = sequence;
    this.storageGroup = storageGroup;
  }

  @Override
  protected void doCompaction() throws Exception {
  }
}
