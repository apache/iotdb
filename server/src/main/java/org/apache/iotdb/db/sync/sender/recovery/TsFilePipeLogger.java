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
 *
 */
package org.apache.iotdb.db.sync.sender.recovery;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.sync.utils.SyncConstant;
import org.apache.iotdb.commons.sync.utils.SyncPathUtil;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.sync.sender.pipe.TsFilePipe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;

public class TsFilePipeLogger {
  private static final Logger logger = LoggerFactory.getLogger(TsFilePipeLogger.class);

  private final String pipeDir;
  private final String tsFileDir;

  public TsFilePipeLogger(TsFilePipe tsFilePipe) {
    pipeDir = SyncPathUtil.getSenderPipeDir(tsFilePipe.getName(), tsFilePipe.getCreateTime());
    tsFileDir = SyncPathUtil.getSenderFileDataDir(tsFilePipe.getName(), tsFilePipe.getCreateTime());
  }

  public boolean isHardlinkExist(File tsFile) {
    File link = new File(tsFileDir, getRelativeFilePath(tsFile));
    return link.exists();
  }

  /** make hard link for tsfile * */
  public File createTsFileAndModsHardlink(File tsFile, long modsOffset) throws IOException {
    File mods = new File(tsFile.getPath() + ModificationFile.FILE_SUFFIX);

    if (mods.exists()) {
      File modsHardLink = createHardLink(mods);
      if (modsOffset != 0L) {
        serializeModsOffset(
            new File(modsHardLink.getPath() + SyncConstant.MODS_OFFSET_FILE_SUFFIX), modsOffset);
      }
    } else if (modsOffset != 0L) {
      logger.warn(
          String.format(
              "Can not find %s mods to create hard link. The mods offset is %d.",
              mods.getPath(), modsOffset));
    }
    createTsFileResourceHardlink(tsFile);
    return createTsFileHardlink(tsFile);
  }

  private void serializeModsOffset(File modsOffsetFile, long modsOffset) {
    try {
      SyncPathUtil.createFile(modsOffsetFile);
      try (BufferedWriter bw = new BufferedWriter(new FileWriter(modsOffsetFile))) {
        bw.write(String.valueOf(modsOffset));
        bw.flush();
      }
    } catch (IOException e) {
      logger.warn(
          String.format(
              "Serialize mods offset in %s error. The mods offset is %d",
              modsOffsetFile.getPath(), modsOffset));
    }
  }

  public File createTsFileHardlink(File tsFile) throws IOException {
    return createHardLink(tsFile);
  }

  public void createTsFileResourceHardlink(File tsFile) throws IOException {
    File tsFileResource = new File(tsFile.getPath() + TsFileResource.RESOURCE_SUFFIX);
    try {
      createHardLink(tsFileResource);
    } catch (IOException e) {
      logger.warn(
          String.format(
              "Record tsfile resource %s on disk error, make a empty to close it.",
              tsFileResource.getPath()));
      SyncPathUtil.createFile(
          new File(tsFileDir, getRelativeFilePath(tsFileResource))); // create an empty resource
    }
  }

  private File createHardLink(File file) throws IOException {
    File link = new File(tsFileDir, getRelativeFilePath(file));
    if (!link.getParentFile().exists()) {
      link.getParentFile().mkdirs();
    }

    Path sourcePath = FileSystems.getDefault().getPath(file.getAbsolutePath());
    Path linkPath = FileSystems.getDefault().getPath(link.getAbsolutePath());
    Files.createLink(linkPath, sourcePath);
    return link;
  }

  private String getRelativeFilePath(File file) {
    StringBuilder builder = new StringBuilder(file.getName());
    while (!file.getName().equals(IoTDBConstant.SEQUENCE_FLODER_NAME)
        && !file.getName().equals(IoTDBConstant.UNSEQUENCE_FLODER_NAME)) {
      file = file.getParentFile();
      builder =
          new StringBuilder(file.getName())
              .append(IoTDBConstant.FILE_NAME_SEPARATOR)
              .append(builder);
    }
    return builder.toString();
  }

  public void finishCollect() {
    try {
      if (SyncPathUtil.createFile(new File(pipeDir, SyncConstant.FINISH_COLLECT_LOCK_NAME))) {
        logger.info("Create finish collecting Lock file in {}.", pipeDir);
      }
    } catch (IOException e) {
      logger.warn(String.format("Can not make lock file in %s, because %s", pipeDir, e));
    }
  }

  public boolean isCollectFinished() {
    return new File(pipeDir, SyncConstant.FINISH_COLLECT_LOCK_NAME).exists();
  }

  public void clear() throws IOException {
    File pipeDir = new File(this.pipeDir);
    if (pipeDir.exists()) {
      FileUtils.deleteDirectory(pipeDir);
    }
  }
}
