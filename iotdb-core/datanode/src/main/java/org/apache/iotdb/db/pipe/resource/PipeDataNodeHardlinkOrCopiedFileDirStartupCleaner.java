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

import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.resource.PipeSnapshotResourceManager;
import org.apache.iotdb.db.conf.IoTDBDescriptor;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class PipeDataNodeHardlinkOrCopiedFileDirStartupCleaner {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeDataNodeHardlinkOrCopiedFileDirStartupCleaner.class);

  /**
   * Delete the data directory and all of its subdirectories that contain the
   * PipeConfig.PIPE_TSFILE_DIR_NAME directory.
   */
  public static void clean() {
    cleanTsFileDir();
    cleanSnapshotDir();
  }

  private static void cleanTsFileDir() {
    for (final String dataDir : IoTDBDescriptor.getInstance().getConfig().getDataDirs()) {
      final File pipeHardLinkDir =
          new File(
              dataDir + File.separator + PipeConfig.getInstance().getPipeHardlinkBaseDirName());
      if (pipeHardLinkDir.isDirectory()) {
        LOGGER.info(
            "Pipe hardlink dir found, deleting it: {}, result: {}",
            pipeHardLinkDir,
            FileUtils.deleteQuietly(pipeHardLinkDir));
      }
    }
  }

  private static void cleanSnapshotDir() {
    final File pipeConsensusDir =
        new File(
            IoTDBDescriptor.getInstance().getConfig().getConsensusDir()
                + File.separator
                + PipeSnapshotResourceManager.PIPE_SNAPSHOT_DIR_NAME);
    if (pipeConsensusDir.isDirectory()) {
      LOGGER.info("Pipe snapshot dir found, deleting it: {},", pipeConsensusDir);
      org.apache.iotdb.commons.utils.FileUtils.deleteFileOrDirectory(pipeConsensusDir);
    }
  }

  private PipeDataNodeHardlinkOrCopiedFileDirStartupCleaner() {
    // util class
  }
}
