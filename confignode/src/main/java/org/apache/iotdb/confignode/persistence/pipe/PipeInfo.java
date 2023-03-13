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

public class PipeInfo implements SnapshotProcessor {

  private final PipePluginInfo pipePluginInfo;

  public PipeInfo() throws IOException {
    pipePluginInfo = new PipePluginInfo();
  }

  public PipePluginInfo getPipePluginInfo() {
    return pipePluginInfo;
  }

  @Override
  public boolean processTakeSnapshot(File snapshotDir) throws IOException {
    return pipePluginInfo.processTakeSnapshot(snapshotDir);
  }

  @Override
  public void processLoadSnapshot(File snapshotDir) throws IOException {
    pipePluginInfo.processLoadSnapshot(snapshotDir);
  }
}
