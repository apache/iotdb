/*
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
package org.apache.iotdb.db.metadata.logfile;

import java.io.*;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.*;

import org.apache.commons.io.FileUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.MetadataConstant;
import org.apache.iotdb.db.metadata.MetadataOperationType;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.MNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.StorageGroupMNode;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.*;
import org.apache.iotdb.db.writelog.io.LogWriter;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MLogWriter implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(MLogWriter.class);
  private File logFile;
  private LogWriter logWriter;
  private int logNum;
  private ByteBuffer mlogBuffer = ByteBuffer.allocate(
    IoTDBDescriptor.getInstance().getConfig().getMlogBufferSize());

  public MLogWriter(String schemaDir, String logFileName) throws IOException {
    File metadataDir = SystemFileFactory.INSTANCE.getFile(schemaDir);
    if (!metadataDir.exists()) {
      if (metadataDir.mkdirs()) {
        logger.info("create schema folder {}.", metadataDir);
      } else {
        logger.warn("create schema folder {} failed.", metadataDir);
      }
    }

    logFile = SystemFileFactory.INSTANCE.getFile(schemaDir + File.separator + logFileName);
    // always flush
    logWriter = new LogWriter(logFile, 0L);
  }

  public MLogWriter(String logFilePath) throws IOException {
    logFile = SystemFileFactory.INSTANCE.getFile(logFilePath);
    // always flush
    logWriter = new LogWriter(logFile, 0L);
  }

  @Override
  public void close() throws IOException {
    sync();
    logWriter.close();
  }

  private void sync() {
    try {
      logWriter.write(mlogBuffer);
    } catch (IOException e) {
      logger.error("MLog {} sync failed, change system mode to read-only", logFile.getAbsoluteFile(), e);
      IoTDBDescriptor.getInstance().getConfig().setReadOnly(true);
    }
    mlogBuffer.clear();
  }

  private void putLog(PhysicalPlan plan) {
    mlogBuffer.mark();
    try {
      plan.serialize(mlogBuffer);
    } catch (BufferOverflowException e) {
      logger.debug("MLog {} BufferOverflow !", plan.getOperatorType(), e);
      mlogBuffer.reset();
      sync();
      plan.serialize(mlogBuffer);
    }
    logNum ++;
  }

  public void createTimeseries(CreateTimeSeriesPlan createTimeSeriesPlan) throws IOException {
    try {
      putLog(createTimeSeriesPlan);
    } catch (BufferOverflowException e) {
      throw new IOException(
        "Log cannot fit into buffer, please increase mlog_buffer_size", e);
    }
  }

  public void deleteTimeseries(DeleteTimeSeriesPlan deleteTimeSeriesPlan) throws IOException {
    try {
      putLog(deleteTimeSeriesPlan);
    } catch (BufferOverflowException e) {
      throw new IOException(
        "Log cannot fit into buffer, please increase mlog_buffer_size", e);
    }
  }

  public void setStorageGroup(PartialPath storageGroup) throws IOException {
    try {
      SetStorageGroupPlan plan = new SetStorageGroupPlan(storageGroup);
      putLog(plan);
    } catch (BufferOverflowException e) {
      throw new IOException(
        "Log cannot fit into buffer, please increase mlog_buffer_size", e);
    }
  }

  public void deleteStorageGroup(PartialPath storageGroup) throws IOException {
    try {
      DeleteStorageGroupPlan plan = new DeleteStorageGroupPlan(Collections.singletonList(storageGroup));
      putLog(plan);
    } catch (BufferOverflowException e) {
      throw new IOException(
        "Log cannot fit into buffer, please increase mlog_buffer_size", e);
    }
  }

  public void setTTL(PartialPath storageGroup, long ttl) throws IOException {
    try {
      SetTTLPlan plan = new SetTTLPlan(storageGroup, ttl);
      putLog(plan);
    } catch (BufferOverflowException e) {
      throw new IOException(
        "Log cannot fit into buffer, please increase mlog_buffer_size", e);
    }
  }

  public void changeOffset(PartialPath path, long offset) throws IOException {
    try {
      ChangeTagOffsetPlan plan = new ChangeTagOffsetPlan(path, offset);
      putLog(plan);
    } catch (BufferOverflowException e) {
      throw new IOException(
        "Log cannot fit into buffer, please increase mlog_buffer_size", e);
    }
  }

  public void changeAlias(PartialPath path, String alias) throws IOException {
    try {
      ChangeAliasPlan plan = new ChangeAliasPlan(path, alias);
      putLog(plan);
    } catch (BufferOverflowException e) {
      throw new IOException(
        "Log cannot fit into buffer, please increase mlog_buffer_size", e);
    }
  }

  public void serializeMNode(MNode node) throws IOException {
    try {
      int childSize = 0;
      if (node.getChildren() != null) {
        childSize = node.getChildren().size();
      }
      MNodePlan plan = new MNodePlan(node.getName(), childSize);
      putLog(plan);
    } catch (BufferOverflowException e) {
      throw new IOException(
        "Log cannot fit into buffer, please increase mlog_buffer_size", e);
    }
  }

  public void serializeMeasurementMNode(MeasurementMNode node) throws IOException {
    try {
      int childSize = 0;
      if (node.getChildren() != null) {
        childSize = node.getChildren().size();
      }
      MeasurementMNodePlan plan = new MeasurementMNodePlan(node.getName(), node.getAlias(),
        node.getOffset(), childSize, node.getSchema());
      putLog(plan);
    } catch (BufferOverflowException e) {
      throw new IOException(
        "Log cannot fit into buffer, please increase mlog_buffer_size", e);
    }
  }

  public void serializeStorageGroupMNode(StorageGroupMNode node) throws IOException {
    try {
      int childSize = 0;
      if (node.getChildren() != null) {
        childSize = node.getChildren().size();
      }
      StorageGroupMNodePlan plan = new StorageGroupMNodePlan(node.getName(), node.getDataTTL(), childSize);
      putLog(plan);
    } catch (BufferOverflowException e) {
      throw new IOException(
        "Log cannot fit into buffer, please increase mlog_buffer_size", e);
    }
  }

  public static void upgradeMLog(String schemaDir, String logFileName) throws IOException {
    File logFile = SystemFileFactory.INSTANCE.getFile(schemaDir + File.separator + logFileName);
    File tmpLogFile = SystemFileFactory.INSTANCE.getFile(logFile.getAbsolutePath() + ".tmp");
    File oldLogFile = SystemFileFactory.INSTANCE.getFile(
        schemaDir + File.separator + MetadataConstant.METADATA_OLD_LOG);

    if (oldLogFile.exists()) {
      try (MLogWriter mLogWriter = new MLogWriter(schemaDir, logFileName + ".tmp");
        MLogTxtReader MLogTxtReader = new MLogTxtReader(schemaDir, MetadataConstant.METADATA_OLD_LOG)) {
        // upgrade from old character log file to new binary mlog
        while (MLogTxtReader.hasNext()) {
          String cmd = MLogTxtReader.next();
          try {
            mLogWriter.operation(cmd);
          } catch (MetadataException e) {
            logger.error("failed to upgrade cmd {}.", cmd, e);
          }
        }
      }
    } else if (!logFile.exists() && !tmpLogFile.exists()) {
      // if both old mlog.bin and mlog.bin.tmp do not exist, nothing to do
    } else if (!logFile.exists() && tmpLogFile.exists()) {
      // if old mlog.bin doesn't exist but mlog.bin.tmp exists, rename tmp file to mlog
      FSFactoryProducer.getFSFactory().moveFile(tmpLogFile, logFile);
    } else if (tmpLogFile.exists()) {
      // if both old mlog.bin and mlog.bin.tmp exist, delete mlog.bin.tmp
      if (!tmpLogFile.delete()) {
        throw new IOException("Deleting " + tmpLogFile + "failed.");
      }
    }

    // do some clean job

    // remove old mlog.txt and mlog.txt.tmp
    File tmpOldLogFile = SystemFileFactory.INSTANCE.getFile(oldLogFile.getAbsolutePath()
      + ".tmp");

    if (oldLogFile.exists() && !oldLogFile.delete()) {
      throw new IOException("Deleting old mlog.txt " + oldLogFile + "failed.");
    }

    if (tmpLogFile.exists() && !tmpLogFile.delete()) {
      throw new IOException("Deleting old mlog.txt.tmp " + oldLogFile + "failed.");
    }

    // rename mlog.bin.tmp to mlog.bin
    FileUtils.moveFile(tmpLogFile, logFile);
  }

  public void clear() throws IOException {
    sync();
    logWriter.close();
    mlogBuffer.clear();
    if (logFile != null) {
      if (logFile.exists()) {
        Files.delete(logFile.toPath());
      }
    }
    logNum = 0;
    logWriter = new LogWriter(logFile, 0L);
  }

  public int getLogNum() {
    return logNum;
  }

  /**
   * only used for initialize a mlog file writer.
   */
  public void setLogNum(int number) {
    logNum = number;
  }

  /**
   * upgrade from mlog.txt to mlog.bin
   * @param cmd, the old meta operation
   * @throws IOException
   * @throws MetadataException
   */
  public void operation(String cmd) throws IOException, MetadataException {
    // see createTimeseries() to get the detailed format of the cmd
    String[] args = cmd.trim().split(",", -1);
    switch (args[0]) {
      case MetadataOperationType.CREATE_TIMESERIES:
        Map<String, String> props = null;
        if (!args[5].isEmpty()) {
          String[] keyValues = args[5].split("&");
          String[] kv;
          props = new HashMap<>();
          for (String keyValue : keyValues) {
            kv = keyValue.split("=");
            props.put(kv[0], kv[1]);
          }
        }

        String alias = null;
        if (!args[6].isEmpty()) {
          alias = args[6];
        }
        long offset = -1L;
        if (!args[7].isEmpty()) {
          offset = Long.parseLong(args[7]);
        }

        CreateTimeSeriesPlan plan = new CreateTimeSeriesPlan(new PartialPath(args[1]),
            TSDataType.deserialize(Short.parseShort(args[2])),
            TSEncoding.deserialize(Short.parseShort(args[3])),
            CompressionType.deserialize(Short.parseShort(args[4])), props, null, null, alias);
        plan.setTagOffset(offset);

        createTimeseries(plan);
        break;
      case MetadataOperationType.DELETE_TIMESERIES:
        DeleteTimeSeriesPlan deleteTimeSeriesPlan =
            new DeleteTimeSeriesPlan(Collections.singletonList(new PartialPath(args[1])));
        deleteTimeseries(deleteTimeSeriesPlan);
        break;
      case MetadataOperationType.SET_STORAGE_GROUP:
        setStorageGroup(new PartialPath(args[1]));
        break;
      case MetadataOperationType.DELETE_STORAGE_GROUP:
        for(int i = 1; i <= args.length; i++) {
          deleteStorageGroup(new PartialPath(args[i]));
        }
        break;
      case MetadataOperationType.SET_TTL:
        setTTL(new PartialPath(args[1]), Long.parseLong(args[2]));
        break;
      case MetadataOperationType.CHANGE_OFFSET:
        changeOffset(new PartialPath(args[1]), Long.parseLong(args[2]));
        break;
      case MetadataOperationType.CHANGE_ALIAS:
        changeAlias(new PartialPath(args[1]), args[2]);
        break;
      default:
        logger.error("Unrecognizable command {}", cmd);
    }
  }

  public void force() throws IOException {
    logWriter.force();
  }
}
