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

package org.apache.iotdb.confignode.manager.pipe.resource.snapshot;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.pipe.resource.PipeSnapshotResourceManager;

import java.util.Collections;
import java.util.HashSet;

public class PipeConfigNodeSnapshotResourceManager extends PipeSnapshotResourceManager {

  private PipeConfigNodeSnapshotResourceManager() {
    super(new HashSet<>(Collections.singletonList(IoTDBConstant.CONSENSUS_FOLDER_NAME)));
  }

  private static class PipeConfigNodeSnapshotResourceManagerHolder {
    private static final PipeConfigNodeSnapshotResourceManager INSTANCE =
        new PipeConfigNodeSnapshotResourceManager();
  }

  public static synchronized PipeConfigNodeSnapshotResourceManager getInstance() {
    return PipeConfigNodeSnapshotResourceManagerHolder.INSTANCE;
  }
}
