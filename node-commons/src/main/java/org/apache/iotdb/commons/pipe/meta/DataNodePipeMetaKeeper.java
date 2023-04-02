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

import org.apache.iotdb.commons.pipe.plugin.meta.DataNodePipePluginMetaKeeper;
import org.apache.iotdb.commons.pipe.task.meta.DataNodePipeTaskMetaKeeper;

public class DataNodePipeMetaKeeper extends PipeMetaKeeper {

  DataNodePipeTaskMetaKeeper pipeTaskMetaKeeper;
  DataNodePipePluginMetaKeeper pipePluginMetaKeeper;

  private DataNodePipeMetaKeeper() {
    super();
    this.pipeTaskMetaKeeper = new DataNodePipeTaskMetaKeeper();
    this.pipePluginMetaKeeper = new DataNodePipePluginMetaKeeper();
  }

  public static DataNodePipeMetaKeeper getInstance() {
    return DataNodePipeMetaKeeper.DataNodePipeMetaKeeperHolder.INSTANCE;
  }

  private static class DataNodePipeMetaKeeperHolder {

    private static final DataNodePipeMetaKeeper INSTANCE = new DataNodePipeMetaKeeper();

    private DataNodePipeMetaKeeperHolder() {
      // Empty constructor
    }
  }
}
