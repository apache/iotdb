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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Objects;

public class PipeInfo implements SnapshotProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeInfo.class);

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
    return pipeTaskInfo.processTakeSnapshot(snapshotDir)
        && pipePluginInfo.processTakeSnapshot(snapshotDir);
  }

  @Override
  public void processLoadSnapshot(File snapshotDir) throws IOException {
    Exception loadPipeTaskInfoException = null;
    Exception loadPipePluginInfoException = null;

    try {
      pipeTaskInfo.processLoadSnapshot(snapshotDir);
    } catch (Exception ex) {
      LOGGER.error("Failed to load pipe task info from snapshot", ex);
      loadPipeTaskInfoException = ex;
    }

    try {
      pipePluginInfo.processLoadSnapshot(snapshotDir);
    } catch (Exception ex) {
      LOGGER.error("Failed to load pipe plugin info from snapshot", ex);
      loadPipePluginInfoException = ex;
    }

    if (loadPipeTaskInfoException != null || loadPipePluginInfoException != null) {
      throw new IOException(
          "Failed to load pipe info from snapshot, "
              + "loadPipeTaskInfoException="
              + loadPipeTaskInfoException
              + ", loadPipePluginInfoException="
              + loadPipePluginInfoException);
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

  @Override
  public String toString() {
    return "PipeInfo{"
        + "pipePluginInfo="
        + pipePluginInfo
        + ", pipeTaskInfo="
        + pipeTaskInfo
        + '}';
  }
}
