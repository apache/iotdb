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

package org.apache.iotdb.confignode.manager.pipe.resource;

import org.apache.iotdb.commons.pipe.resource.snapshot.PipeSnapshotResourceManager;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class PipeConfigNodeCopiedFileDirStartupCleaner {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeConfigNodeCopiedFileDirStartupCleaner.class);

  /** Delete the snapshot directory of pipe. */
  public static void clean() {
    File pipeConsensusDir =
        new File(
            ConfigNodeDescriptor.getInstance().getConf().getConsensusDir()
                + File.separator
                + PipeSnapshotResourceManager.PIPE_SNAPSHOT_DIR_NAME);
    if (pipeConsensusDir.isDirectory()) {
      LOGGER.info("Pipe snapshot dir found, deleting it: {},", pipeConsensusDir);
      FileUtils.deleteFileOrDirectory(pipeConsensusDir);
    }
  }

  private PipeConfigNodeCopiedFileDirStartupCleaner() {
    // util class
  }
}
