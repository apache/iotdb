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

package org.apache.iotdb.confignode.persistence.pipe;

import org.apache.iotdb.commons.snapshot.SnapshotProcessor;

import java.io.File;
import java.io.IOException;
import java.util.Objects;

public class PipeInfo implements SnapshotProcessor {

  private final PipePluginInfo pipePluginInfo;
  private final PipeTaskInfo pipeTaskInfo;

  public PipeInfo() throws IOException {
    pipePluginInfo = new PipePluginInfo();
    pipeTaskInfo = new PipeTaskInfo();
  }

  public PipePluginInfo getPipePluginInfo() {
    return pipePluginInfo;
  }

  public PipeTaskInfo getPipeTaskInfo() {
    return pipeTaskInfo;
  }

  /////////////////////////////////  SnapshotProcessor  /////////////////////////////////

  @Override
  public boolean processTakeSnapshot(File snapshotDir) throws IOException {
    pipeTaskInfo.acquirePipeTaskInfoLock();
    pipePluginInfo.acquirePipePluginInfoLock();
    try {
      return pipeTaskInfo.processTakeSnapshot(snapshotDir)
          && pipePluginInfo.processTakeSnapshot(snapshotDir);
    } finally {
      pipePluginInfo.releasePipePluginInfoLock();
      pipeTaskInfo.releasePipeTaskInfoLock();
    }
  }

  @Override
  public void processLoadSnapshot(File snapshotDir) throws IOException {
    pipeTaskInfo.acquirePipeTaskInfoLock();
    pipePluginInfo.acquirePipePluginInfoLock();
    try {
      pipeTaskInfo.processLoadSnapshot(snapshotDir);
      pipePluginInfo.processLoadSnapshot(snapshotDir);
    } finally {
      pipePluginInfo.releasePipePluginInfoLock();
      pipeTaskInfo.releasePipeTaskInfoLock();
    }
  }

  /////////////////////////////////  equals & hashCode  /////////////////////////////////

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PipeInfo pipeInfo = (PipeInfo) o;
    return Objects.equals(pipePluginInfo, pipeInfo.pipePluginInfo)
        && Objects.equals(pipeTaskInfo, pipeInfo.pipeTaskInfo);
  }

  @Override
  public int hashCode() {
    return Objects.hash(pipePluginInfo, pipeTaskInfo);
  }
}
