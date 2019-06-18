/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.engine.filenodeV2;

import java.io.File;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.directories.Directories;
import org.apache.iotdb.db.engine.bufferwriteV2.BufferWriteProcessorV2;
import org.apache.iotdb.db.engine.filenode.CopyOnWriteLinkedList;
import org.apache.iotdb.db.engine.filenode.TsFileResource;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.schema.FileSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileNodeProcessorV2 {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileNodeProcessorV2.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private static final MManager mManager = MManager.getInstance();
  private static final Directories directories = Directories.getInstance();

  private FileSchema fileSchema;

  /**
   * device -> tsfile list
   */
  private Map<String, List<TsFileResourceV2>> invertedIndexOfFiles = new HashMap<>();

  private List<TsFileResource> newFileNodes;

  private BufferWriteProcessorV2 activeBufferWriteProcessor = null;

  /**
   * device -> global latest timestamp of each device
   */
  private Map<String, Long> latestTimeMap = new HashMap<>();

  /**
   * device -> largest timestamp of the latest memtable to be submitted to flush
   */
  private Map<String, Long> latestFlushTimeMap = new HashMap<>();

  private String storageGroup;

  private CopyOnWriteLinkedList<BufferWriteProcessorV2> closingBufferWriteProcessor = new CopyOnWriteLinkedList<>();

  private final ReadWriteLock lock;

  public FileNodeProcessorV2(String baseDir, String storageGroup) {
    this.storageGroup = storageGroup;
    lock = new ReentrantReadWriteLock();

    File storageGroupDir = new File(baseDir + storageGroup);
    if (!storageGroupDir.exists()) {
      storageGroupDir.mkdir();
      LOGGER.info(
          "The directory of the storage group {} doesn't exist. Create a new " +
              "directory {}", storageGroup, storageGroupDir.getAbsolutePath());
    }
  }

  public boolean insert(TSRecord tsRecord) {

    lock.writeLock().lock();
    boolean result = true;

    try {

      // create a new BufferWriteProcessor
      if (activeBufferWriteProcessor == null) {
        String baseDir = directories.getNextFolderForTsfile();
        String filePath = Paths.get(baseDir, storageGroup, tsRecord.time + "").toString();
        activeBufferWriteProcessor = new BufferWriteProcessorV2(storageGroup, new File(filePath),
            fileSchema);

        // TODO check if the disk is full
      }

      // init map
      latestTimeMap.putIfAbsent(tsRecord.deviceId, Long.MIN_VALUE);
      latestFlushTimeMap.putIfAbsent(tsRecord.deviceId, Long.MIN_VALUE);

      if (tsRecord.time > latestFlushTimeMap.get(tsRecord.deviceId)) {
        // write BufferWrite
        result = activeBufferWriteProcessor.write(tsRecord);

        // try to update the latest time of the device of this tsRecord
        if (result && latestTimeMap.get(tsRecord.deviceId) < tsRecord.time) {
          latestTimeMap.put(tsRecord.deviceId, tsRecord.time);
        }

        // check memtable size and may flush the workMemtable
        if (activeBufferWriteProcessor.shouldFlush()) {
          activeBufferWriteProcessor.flush();

          // update the largest time stamp in the last flushing memtable
          latestFlushTimeMap.put(tsRecord.deviceId, tsRecord.time);
        }

        // check file size and may close the BufferWrite
        if (activeBufferWriteProcessor.shouldClose()) {
          closingBufferWriteProcessor.add(activeBufferWriteProcessor);
          activeBufferWriteProcessor = null;
        }
      } else {
        // TODO write to overflow
      }
    } catch (Exception e) {

    } finally {
      lock.writeLock().unlock();
    }

    return result;

  }

}
