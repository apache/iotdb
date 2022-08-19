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

import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.SystemStatus;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.ActivateTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.AppendTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.AutoCreateDeviceMNodePlan;
import org.apache.iotdb.db.qp.physical.sys.ChangeAliasPlan;
import org.apache.iotdb.db.qp.physical.sys.ChangeTagOffsetPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateAlignedTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.DeleteStorageGroupPlan;
import org.apache.iotdb.db.qp.physical.sys.DeleteTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.DropTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.MNodePlan;
import org.apache.iotdb.db.qp.physical.sys.MeasurementMNodePlan;
import org.apache.iotdb.db.qp.physical.sys.PruneTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.db.qp.physical.sys.SetTTLPlan;
import org.apache.iotdb.db.qp.physical.sys.SetTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.StorageGroupMNodePlan;
import org.apache.iotdb.db.qp.physical.sys.UnsetTemplatePlan;
import org.apache.iotdb.db.utils.writelog.LogWriter;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Collections;

public class MLogWriter implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(MLogWriter.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private final File logFile;
  private LogWriter logWriter;
  private int logNum;
  private final ByteBuffer mlogBuffer =
      ByteBuffer.allocate(IoTDBDescriptor.getInstance().getConfig().getMlogBufferSize());

  private static final String LOG_TOO_LARGE_INFO =
      "Log cannot fit into buffer, please increase mlog_buffer_size";

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
    logWriter = new LogWriter(logFile, config.getSyncMlogPeriodInMs() == 0);
  }

  public MLogWriter(String logFilePath) throws IOException {
    logFile = SystemFileFactory.INSTANCE.getFile(logFilePath);
    logWriter = new LogWriter(logFile, config.getSyncMlogPeriodInMs() == 0);
  }

  @Override
  public void close() throws IOException {
    logWriter.close();
  }

  public synchronized void copyTo(File targetFile) throws IOException {
    // flush the mlogBuffer
    sync();
    // flush the os buffer
    force();
    FileUtils.copyFile(logFile, targetFile);
  }

  private void sync() {
    int retryCnt = 0;
    mlogBuffer.mark();
    while (true) {
      try {
        logWriter.write(mlogBuffer);
        break;
      } catch (IOException e) {
        if (retryCnt < 3) {
          logger.warn("MLog {} sync failed, retry it again", logFile.getAbsoluteFile(), e);
          mlogBuffer.reset();
          retryCnt++;
        } else {
          logger.error(
              "MLog {} sync failed, change system mode to error", logFile.getAbsoluteFile(), e);
          IoTDBDescriptor.getInstance().getConfig().setSystemStatus(SystemStatus.ERROR);
          break;
        }
      }
    }
    mlogBuffer.clear();
  }

  public synchronized void putLog(PhysicalPlan plan) throws IOException {
    try {
      plan.serialize(mlogBuffer);
      sync();
      logNum++;
    } catch (BufferOverflowException e) {
      throw new IOException(LOG_TOO_LARGE_INFO, e);
    }
  }

  public void createTimeseries(CreateTimeSeriesPlan createTimeSeriesPlan) throws IOException {
    putLog(createTimeSeriesPlan);
  }

  public void createAlignedTimeseries(CreateAlignedTimeSeriesPlan createAlignedTimeSeriesPlan)
      throws IOException {
    putLog(createAlignedTimeSeriesPlan);
  }

  public void deleteTimeseries(DeleteTimeSeriesPlan deleteTimeSeriesPlan) throws IOException {
    putLog(deleteTimeSeriesPlan);
  }

  public void setStorageGroup(PartialPath storageGroup) throws IOException {
    SetStorageGroupPlan plan = new SetStorageGroupPlan(storageGroup);
    putLog(plan);
  }

  public void deleteStorageGroup(PartialPath storageGroup) throws IOException {
    DeleteStorageGroupPlan plan =
        new DeleteStorageGroupPlan(Collections.singletonList(storageGroup));
    putLog(plan);
  }

  public void setTTL(PartialPath storageGroup, long ttl) throws IOException {
    SetTTLPlan plan = new SetTTLPlan(storageGroup, ttl);
    putLog(plan);
  }

  public void changeOffset(PartialPath path, long offset) throws IOException {
    ChangeTagOffsetPlan plan = new ChangeTagOffsetPlan(path, offset);
    putLog(plan);
  }

  public void changeAlias(PartialPath path, String alias) throws IOException {
    ChangeAliasPlan plan = new ChangeAliasPlan(path, alias);
    putLog(plan);
  }

  public void createSchemaTemplate(CreateTemplatePlan plan) throws IOException {
    putLog(plan);
  }

  public void appendSchemaTemplate(AppendTemplatePlan plan) throws IOException {
    putLog(plan);
  }

  public void pruneSchemaTemplate(PruneTemplatePlan plan) throws IOException {
    putLog(plan);
  }

  public void setSchemaTemplate(SetTemplatePlan plan) throws IOException {
    putLog(plan);
  }

  public void unsetSchemaTemplate(UnsetTemplatePlan plan) throws IOException {
    putLog(plan);
  }

  public void dropSchemaTemplate(DropTemplatePlan plan) throws IOException {
    putLog(plan);
  }

  public void autoCreateDeviceMNode(AutoCreateDeviceMNodePlan plan) throws IOException {
    putLog(plan);
  }

  public void serializeMNode(IMNode node) throws IOException {
    int childSize = 0;
    if (node.getChildren() != null) {
      childSize = node.getChildren().size();
    }
    MNodePlan plan = new MNodePlan(node.getName(), childSize);
    putLog(plan);
  }

  public void serializeMeasurementMNode(IMeasurementMNode node) throws IOException {
    int childSize = 0;
    if (node.getChildren() != null) {
      childSize = node.getChildren().size();
    }
    MeasurementMNodePlan plan =
        new MeasurementMNodePlan(
            node.getName(), node.getAlias(), node.getOffset(), childSize, node.getSchema());
    putLog(plan);
  }

  public void serializeStorageGroupMNode(IStorageGroupMNode node) throws IOException {
    int childSize = 0;
    if (node.getChildren() != null) {
      childSize = node.getChildren().size();
    }
    StorageGroupMNodePlan plan =
        new StorageGroupMNodePlan(node.getName(), node.getDataTTL(), childSize);
    putLog(plan);
  }

  public void setUsingSchemaTemplate(PartialPath path) throws IOException {
    ActivateTemplatePlan plan = new ActivateTemplatePlan(path);
    putLog(plan);
  }

  public synchronized void clear() throws IOException {
    sync();
    logWriter.close();
    mlogBuffer.clear();
    if (logFile != null && logFile.exists()) {
      Files.delete(logFile.toPath());
    }
    logNum = 0;
    logWriter = new LogWriter(logFile, false);
  }

  public synchronized int getLogNum() {
    return logNum;
  }

  /** only used for initialize a mlog file writer. */
  public synchronized void setLogNum(int number) {
    logNum = number;
  }

  public synchronized void force() throws IOException {
    logWriter.force();
  }

  public static synchronized PhysicalPlan convertFromString(String str) {
    String[] words = str.split(",");
    switch (words[0]) {
      case "2":
        return new MeasurementMNodePlan(
            words[1],
            "".equals(words[2]) ? null : words[2],
            Long.parseLong(words[words.length - 2]),
            Integer.parseInt(words[words.length - 1]),
            new MeasurementSchema(
                words[1],
                TSDataType.values()[Integer.parseInt(words[3])],
                TSEncoding.values()[Integer.parseInt(words[4])],
                CompressionType.values()[Integer.parseInt(words[5])]));
      case "1":
        return new StorageGroupMNodePlan(
            words[1], Long.parseLong(words[2]), Integer.parseInt(words[3]));
      case "0":
        return new MNodePlan(words[1], Integer.parseInt(words[2]));
      default:
        logger.error("unknown cmd {}", str);
    }
    return null;
  }
}
