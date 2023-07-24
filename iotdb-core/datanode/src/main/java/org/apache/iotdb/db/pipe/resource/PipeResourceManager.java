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

package org.apache.iotdb.db.pipe.resource;

import org.apache.iotdb.db.pipe.resource.tsfile.PipeTsFileResourceManager;
import org.apache.iotdb.db.pipe.resource.wal.PipeWALFileResourceManager;
import org.apache.iotdb.db.pipe.resource.wal.PipeWALResourceManager;

public class PipeResourceManager {

  private final PipeTsFileResourceManager pipeTsFileResourceManager;
  private final PipeWALResourceManager pipeWALResourceManager;
  private final PipeWALFileResourceManager pipeWALFileResourceManager;

  public static PipeTsFileResourceManager tsfile() {
    return PipeResourceManagerHolder.INSTANCE.pipeTsFileResourceManager;
  }

  public static PipeWALResourceManager wal() {
    return PipeResourceManagerHolder.INSTANCE.pipeWALResourceManager;
  }

  public static PipeWALFileResourceManager walHardlink() {
    return PipeResourceManagerHolder.INSTANCE.pipeWALFileResourceManager;
  }

  ///////////////////////////// SINGLETON /////////////////////////////

  private PipeResourceManager() {
    pipeTsFileResourceManager = new PipeTsFileResourceManager();
    pipeWALResourceManager = new PipeWALResourceManager();
    pipeWALFileResourceManager = new PipeWALFileResourceManager();
  }

  private static class PipeResourceManagerHolder {
    private static final PipeResourceManager INSTANCE = new PipeResourceManager();
  }
}
