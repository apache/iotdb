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

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.CreateTemplatePlan;
import org.apache.iotdb.db.qp.physical.crud.SetSchemaTemplatePlan;
import org.apache.iotdb.db.qp.physical.crud.UnsetSchemaTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.AutoCreateDeviceMNodePlan;
import org.apache.iotdb.db.qp.physical.sys.ChangeAliasPlan;
import org.apache.iotdb.db.qp.physical.sys.ChangeTagOffsetPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateAlignedTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateContinuousQueryPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.DeleteStorageGroupPlan;
import org.apache.iotdb.db.qp.physical.sys.DeleteTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.DropContinuousQueryPlan;
import org.apache.iotdb.db.qp.physical.sys.MNodePlan;
import org.apache.iotdb.db.qp.physical.sys.MeasurementMNodePlan;
import org.apache.iotdb.db.qp.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.db.qp.physical.sys.SetTTLPlan;
import org.apache.iotdb.db.qp.physical.sys.SetUsingSchemaTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.StorageGroupMNodePlan;
import org.apache.iotdb.db.writelog.io.LogWriter;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.schema.UnaryMeasurementSchema;

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
    logWriter = new LogWriter(logFile, false);
  }

  public MLogWriter(String logFilePath) throws IOException {
    logFile = SystemFileFactory.INSTANCE.getFile(logFilePath);
    logWriter = new LogWriter(logFile, false);
  }

  @Override
  public void close() throws IOException {
    logWriter.close();
  }

  private void sync() {
    try {
      logWriter.write(mlogBuffer);
    } catch (IOException e) {
      logger.error(
          "MLog {} sync failed, change system mode to read-only", logFile.getAbsoluteFile(), e);
      IoTDBDescriptor.getInstance().getConfig().setReadOnly(true);
    }
    mlogBuffer.clear();
  }

  synchronized void putLog(PhysicalPlan plan) throws IOException {
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

  public void createContinuousQuery(CreateContinuousQueryPlan createContinuousQueryPlan)
      throws IOException {
    putLog(createContinuousQueryPlan);
  }

  public void dropContinuousQuery(DropContinuousQueryPlan dropContinuousQueryPlan)
      throws IOException {
    putLog(dropContinuousQueryPlan);
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

  public void setSchemaTemplate(SetSchemaTemplatePlan plan) throws IOException {
    putLog(plan);
  }

  public void unsetSchemaTemplate(UnsetSchemaTemplatePlan plan) throws IOException {
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
    SetUsingSchemaTemplatePlan plan = new SetUsingSchemaTemplatePlan(path);
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
            words[2].equals("") ? null : words[2],
            Long.parseLong(words[words.length - 2]),
            Integer.parseInt(words[words.length - 1]),
            new UnaryMeasurementSchema(
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
