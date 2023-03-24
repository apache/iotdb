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
package org.apache.iotdb.commons.pipe.task.meta;

import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class ConfigNodePipeTaskMetaKeeper extends PipeTaskMetaKeeper {

  public void processTakeSnapshot(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(pipeNameToPipeTaskMetaMap.size(), outputStream);
    for (PipeTaskMeta pipeTaskMeta : pipeNameToPipeTaskMetaMap.values()) {
      ReadWriteIOUtils.write(pipeTaskMeta.serialize(), outputStream);
    }
  }

  public void processLoadSnapshot(InputStream inputStream) throws IOException {
    pipeNameToPipeTaskMetaMap.clear();

    final int pipePluginMetaSize = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < pipePluginMetaSize; i++) {
      final PipeTaskMeta pipeTaskMeta = PipeTaskMeta.deserialize(inputStream);
      addPipeTaskMeta(pipeTaskMeta);
    }
  }

  public static ConfigNodePipeTaskMetaKeeper getInstance() {
    return ConfigNodePipeTaskMetaKeeper.ConfigNodePipeTaskMetaKeeperHolder.INSTANCE;
  }

  private static class ConfigNodePipeTaskMetaKeeperHolder {

    private static final ConfigNodePipeTaskMetaKeeper INSTANCE = new ConfigNodePipeTaskMetaKeeper();

    private ConfigNodePipeTaskMetaKeeperHolder() {
      // Empty constructor
    }
  }
}
