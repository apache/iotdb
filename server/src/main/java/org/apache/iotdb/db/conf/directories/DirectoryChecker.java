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
package org.apache.iotdb.db.conf.directories;

import org.apache.iotdb.commons.exception.ConfigurationException;
import org.apache.iotdb.commons.utils.ProcessIdUtils;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.ArrayList;
import java.util.List;

public class DirectoryChecker {
  private static final Logger logger = LoggerFactory.getLogger(DirectoryChecker.class);
  private static final String LOCK_FILE_NAME = ".iotdb-lock";
  private final List<RandomAccessFile> randomAccessFileList = new ArrayList<>();
  private final List<File> fileList = new ArrayList<>();

  private DirectoryChecker() {}

  public static DirectoryChecker getInstance() {
    return DirectoryCheckerHolder.INSTANCE;
  }

  public void registerDirectory(File dir) throws ConfigurationException, IOException {
    if (dir.exists() && !dir.isDirectory()) {
      throw new ConfigurationException(
          String.format(
              "Unable to create directory %s because there is file under the path, please check configuration and restart.",
              dir.getAbsolutePath()));
    } else if (!dir.exists()) {
      if (!dir.mkdirs()) {
        throw new ConfigurationException(
            String.format(
                "Unable to create directory %s, please check configuration and restart.",
                dir.getAbsolutePath()));
      }
    }
    File file = new File(dir, LOCK_FILE_NAME);
    RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
    FileChannel channel = randomAccessFile.getChannel();
    FileLock lock = null;
    try {
      // Try acquiring the lock without blocking. This method returns
      // null or throws an exception if the file is already locked.
      lock = channel.tryLock();
    } catch (OverlappingFileLockException e) {
      // File is already locked in this thread or virtual machine
    }
    // File is already locked other virtual machine
    if (lock == null) {
      throw new ConfigurationException(
          String.format(
              "Conflict is detected in directory %s, which may be being used by another IoTDB (ProcessId=%s). Please check configuration and restart.",
              dir.getAbsolutePath(), randomAccessFile.readLine()));
    }
    randomAccessFile.writeBytes(ProcessIdUtils.getProcessId());
    // add to list
    fileList.add(file);
    randomAccessFileList.add(randomAccessFile);
  }

  public void deregisterAll() {
    try {
      for (RandomAccessFile randomAccessFile : randomAccessFileList) {
        randomAccessFile.close();
        // it will release lock automatically after close
      }
      for (File file : fileList) {
        FileUtils.delete(file);
      }
    } catch (IOException e) {
      logger.warn("Failed to deregister file lock because {}", e.getMessage(), e);
    }
  }

  private static class DirectoryCheckerHolder {
    private static final DirectoryChecker INSTANCE = new DirectoryChecker();

    private DirectoryCheckerHolder() {}
  }
}
