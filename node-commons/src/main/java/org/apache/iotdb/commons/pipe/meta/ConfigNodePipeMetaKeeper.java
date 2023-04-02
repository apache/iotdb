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
package org.apache.iotdb.commons.pipe.meta;

import org.apache.iotdb.commons.pipe.plugin.meta.ConfigNodePipePluginMetaKeeper;
import org.apache.iotdb.commons.pipe.task.meta.ConfigNodePipeTaskMetaKeeper;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class ConfigNodePipeMetaKeeper extends PipeMetaKeeper {

  private ConfigNodePipeMetaKeeper() {
    super();
    this.pipeTaskMetaKeeper = new ConfigNodePipeTaskMetaKeeper();
    this.pipePluginMetaKeeper = new ConfigNodePipePluginMetaKeeper();
  }

  public void processTakeSnapshot(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(pipeNameToPipeMetaMap.size(), outputStream);
    for (PipeMeta pipeMeta : pipeNameToPipeMetaMap.values()) {
      ReadWriteIOUtils.write(pipeMeta.serialize(), outputStream);
    }
    ((ConfigNodePipePluginMetaKeeper) pipePluginMetaKeeper).processTakeSnapshot(outputStream);
  }

  public void processLoadSnapshot(InputStream inputStream) throws IOException {
    pipeNameToPipeMetaMap.clear();

    final int pipePluginMetaSize = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < pipePluginMetaSize; i++) {
      final PipeMeta pipeMeta = PipeMeta.deserialize(inputStream);
      addPipeMeta(pipeMeta);
    }
    ((ConfigNodePipePluginMetaKeeper) pipePluginMetaKeeper).processLoadSnapshot(inputStream);
  }

  public static ConfigNodePipeMetaKeeper getInstance() {
    return ConfigNodePipeMetaKeeper.ConfigNodePipeMetaKeeperHolder.INSTANCE;
  }

  private static class ConfigNodePipeMetaKeeperHolder {

    private static final ConfigNodePipeMetaKeeper INSTANCE = new ConfigNodePipeMetaKeeper();

    private ConfigNodePipeMetaKeeperHolder() {
      // Empty constructor
    }
  }
}
