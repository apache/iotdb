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

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.pipe.utils.PipeConstant;
import org.apache.iotdb.commons.sync.utils.SyncConstant;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class PipeTsFileHolder {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTsFileHolder.class);

  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private final ConcurrentHashMap<String, AtomicInteger> tsFileReferenceMap =
      new ConcurrentHashMap<>();

  public File increaseFileReference(File tsFile) {
    lock.writeLock().lock();

    try {
      // if the tsfile is already referenced, just return it
      if (tsFileReferenceMap.containsKey(tsFile.getPath())) {
        tsFileReferenceMap.get(tsFile.getPath()).incrementAndGet();
        return tsFile;
      }

      File link = getFileHardlink(tsFile);
      createTsFileHardLink(tsFile);
      tsFileReferenceMap
          .computeIfAbsent(link.getPath(), k -> new AtomicInteger(0))
          .incrementAndGet();
      return link;
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      lock.writeLock().unlock();
    }
  }

  public File increaseFileReference(File tsFile, long modsOffset) {
    lock.writeLock().lock();

    try {
      // if the tsfile is already referenced, just return it
      if (tsFileReferenceMap.containsKey(tsFile.getPath())) {
        tsFileReferenceMap.get(tsFile.getPath()).incrementAndGet();
        return tsFile;
      }

      File link = getFileHardlink(tsFile);
      // hardlink for mods
      createTsFileModHardLink(tsFile, modsOffset);

      // hardlink for tsfileResource
      createTsFileResourceHardLink(tsFile);

      // hardlink for tsfile
      createTsFileHardLink(tsFile);

      tsFileReferenceMap
          .computeIfAbsent(link.getPath(), k -> new AtomicInteger(0))
          .incrementAndGet();
      return link;
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void decreaseFileReference(File tsFile) {
    try {
      if (tsFileReferenceMap.containsKey(tsFile.getPath())) {
        if (tsFileReferenceMap.get(tsFile.getPath()).decrementAndGet() == 0) {
          deleteHardLink(tsFile);
          tsFileReferenceMap.remove(tsFile.getPath());
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void createTsFileHardLink(File tsFile) throws IOException {
    createHardLink(tsFile);
  }

  private void createTsFileResourceHardLink(File tsFile) throws IOException {
    File tsFileResource = new File(tsFile.getPath() + TsFileResource.RESOURCE_SUFFIX);
    try {
      if (tsFileResource.exists()) {
        createHardLink(tsFileResource);
      }
    } catch (IOException e) {
      LOGGER.warn(
          String.format(
              "Record tsfile resource %s on disk error, make a empty to close it.",
              tsFileResource.getPath()));
      createFile(getFileHardlink(tsFileResource));
    }
  }

  private void createTsFileModHardLink(File tsFile, long modsOffset) throws IOException {
    File mods = new File(tsFile.getPath() + ModificationFile.FILE_SUFFIX);

    if (mods.exists()) {
      File modsHardLink = createHardLink(mods);
      if (modsOffset != 0L) {
        serializeModsOffset(
            new File(modsHardLink.getPath() + SyncConstant.MODS_OFFSET_FILE_SUFFIX), modsOffset);
      }
    } else if (modsOffset != 0L) {
      LOGGER.warn(
          String.format(
              "Cannot find %s mods to create hard link. The mods offset is %d.",
              mods.getPath(), modsOffset));
    }
  }

  private void serializeModsOffset(File modsOffsetFile, long modsOffset) {
    try {
      createFile(modsOffsetFile);
      try (BufferedWriter bw = new BufferedWriter(new FileWriter(modsOffsetFile))) {
        bw.write(String.valueOf(modsOffset));
        bw.flush();
      }
    } catch (IOException e) {
      LOGGER.warn(
          String.format(
              "Serialize mods offset in %s error. The mods offset is %d",
              modsOffsetFile.getPath(), modsOffset));
    }
  }

  private File createHardLink(File file) throws IOException {
    File link = getFileHardlink(file);
    if (!link.getParentFile().exists()) {
      boolean ignored = link.getParentFile().mkdirs();
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

  private void deleteHardLink(File tsFile) throws IOException {
    try {
      File tsFileMods = new File(tsFile.getPath() + ModificationFile.FILE_SUFFIX);
      File tsFileResource = new File(tsFile.getPath() + TsFileResource.RESOURCE_SUFFIX);
      Files.deleteIfExists(tsFileMods.toPath());
      Files.deleteIfExists(tsFileResource.toPath());
      Files.deleteIfExists(tsFile.toPath());
    } catch (IOException e) {
      LOGGER.warn(String.format("Delete hard link of tsfile %s error.", tsFile.getPath()));
    }
  }

  private String getPipeTsFileDirPath(File file) throws IOException {
    while (!file.getName().equals(IoTDBConstant.SEQUENCE_FLODER_NAME)
        && !file.getName().equals(IoTDBConstant.UNSEQUENCE_FLODER_NAME)) {
      file = file.getParentFile();
    }
    return file.getParentFile().getCanonicalPath()
        + File.separator
        + PipeConstant.PIPE_TSFILE_DIR_NAME;
  }

  private void createFile(File file) throws IOException {
    if (!file.getParentFile().exists()) {
      boolean ignored = file.getParentFile().mkdirs();
    }
    boolean ignored = file.createNewFile();
  }

  private File getFileHardlink(File file) throws IOException {
    return new File(getPipeTsFileDirPath(file), getRelativeFilePath(file));
  }

  public int getFileReferenceCount(File tsFile) {
    return tsFileReferenceMap.getOrDefault(tsFile.getPath(), new AtomicInteger(0)).get();
  }

  public void clear() {
    for (String dataDir : IoTDBDescriptor.getInstance().getConfig().getDataDirs()) {
      File pipeTsFileDir = new File(dataDir, PipeConstant.PIPE_TSFILE_DIR_NAME);
      if (pipeTsFileDir.exists()) {
        FileUtils.deleteDirectory(pipeTsFileDir);
      }
    }
    tsFileReferenceMap.clear();
  }
}
