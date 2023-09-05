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

package org.apache.iotdb.confignode.persistence.schema;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TemplatePreSetTable {

  private static final Logger LOGGER = LoggerFactory.getLogger(TemplatePreSetTable.class);

  private static final String SNAPSHOT_FILENAME = "template_preset_info.bin";

  private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();

  private final Map<Integer, Set<PartialPath>> templatePreSetMap = new ConcurrentHashMap<>();

  public TemplatePreSetTable() {}

  public boolean isPreSet(int templateId, PartialPath templateSetPath) {
    readWriteLock.readLock().lock();
    try {
      Set<PartialPath> templatePreSetPaths = templatePreSetMap.get(templateId);
      if (templatePreSetPaths == null) {
        return false;
      }
      return templatePreSetPaths.contains(templateSetPath);
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  public void preSetTemplate(int templateId, PartialPath templateSetPath) {
    readWriteLock.writeLock().lock();
    try {
      templatePreSetMap
          .computeIfAbsent(templateId, k -> Collections.synchronizedSet(new HashSet<>()))
          .add(templateSetPath);
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  public boolean removeSetTemplate(int templateId, PartialPath templateSetPath) {
    readWriteLock.writeLock().lock();
    try {
      Set<PartialPath> set = templatePreSetMap.get(templateId);
      if (set == null) {
        return false;
      }
      if (set.remove(templateSetPath)) {
        if (set.isEmpty()) {
          templatePreSetMap.remove(templateId);
        }
        return true;
      } else {
        return false;
      }
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  public boolean processTakeSnapshot(File snapshotDir) throws IOException {
    readWriteLock.writeLock().lock();
    try {
      if (templatePreSetMap.isEmpty()) {
        return true;
      }

      File snapshotFile = new File(snapshotDir, SNAPSHOT_FILENAME);
      if (snapshotFile.exists() && snapshotFile.isFile()) {
        LOGGER.error(
            "Failed to take snapshot of TemplatePreSetTable, because snapshot file [{}] is already exist.",
            snapshotFile.getAbsolutePath());
        return false;
      }
      File tmpFile = new File(snapshotFile.getAbsolutePath() + "-" + UUID.randomUUID());

      try (FileOutputStream fileOutputStream = new FileOutputStream(tmpFile);
          BufferedOutputStream outputStream = new BufferedOutputStream(fileOutputStream)) {
        serialize(outputStream);
        outputStream.flush();
        fileOutputStream.flush();
        outputStream.close();
        fileOutputStream.close();
        return tmpFile.renameTo(snapshotFile);
      } finally {
        for (int retry = 0; retry < 5; retry++) {
          if (!tmpFile.exists() || tmpFile.delete()) {
            break;
          } else {
            LOGGER.warn(
                "Can't delete temporary snapshot file: {}, retrying...", tmpFile.getAbsolutePath());
          }
        }
      }
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  public void processLoadSnapshot(File snapshotDir) throws IOException {
    readWriteLock.writeLock().lock();
    try {
      File snapshotFile = new File(snapshotDir, SNAPSHOT_FILENAME);
      if (!snapshotFile.exists()) {
        return;
      }

      if (!snapshotFile.isFile()) {
        LOGGER.error(
            "Failed to load snapshot of TemplatePreSetTable,snapshot file [{}] is not a valid file.",
            snapshotFile.getAbsolutePath());
        return;
      }

      try (FileInputStream fileInputStream = new FileInputStream(snapshotFile);
          BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream)) {
        // Load snapshot of template preset table
        templatePreSetMap.clear();
        deserialize(bufferedInputStream);
      }
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  private void serialize(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(templatePreSetMap.size(), outputStream);
    for (Map.Entry<Integer, Set<PartialPath>> entry : templatePreSetMap.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), outputStream);
      ReadWriteIOUtils.write(entry.getValue().size(), outputStream);
      for (PartialPath preSetPath : entry.getValue()) {
        ReadWriteIOUtils.write(preSetPath.getFullPath(), outputStream);
      }
    }
  }

  private void deserialize(InputStream inputStream) throws IOException {
    int templateNum = ReadWriteIOUtils.readInt(inputStream);
    while (templateNum > 0) {
      templateNum--;
      int templateId = ReadWriteIOUtils.readInt(inputStream);
      int preSetPathNum = ReadWriteIOUtils.readInt(inputStream);
      Set<PartialPath> set = Collections.synchronizedSet(new HashSet<>());
      while (preSetPathNum > 0) {
        preSetPathNum--;
        try {
          set.add(new PartialPath(ReadWriteIOUtils.readString(inputStream)));
        } catch (IllegalPathException e) {
          // won't happen
        }
      }
      templatePreSetMap.put(templateId, set);
    }
  }
}
