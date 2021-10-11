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

package org.apache.iotdb.cluster.common;

import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.logtypes.AddNodeLog;
import org.apache.iotdb.cluster.log.logtypes.EmptyContentLog;
import org.apache.iotdb.cluster.log.logtypes.LargeTestLog;
import org.apache.iotdb.cluster.partition.PartitionTable;
import org.apache.iotdb.cluster.partition.slot.SlotPartitionTable;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftNode;
import org.apache.iotdb.cluster.rpc.thrift.StartUpStatus;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.qp.executor.PlanExecutor;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.TimeseriesSchema;
import org.apache.iotdb.tsfile.write.schema.UnaryMeasurementSchema;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TestUtils {

  public static long TEST_TIME_OUT_MS = 200;

  public static ByteBuffer seralizePartitionTable = getPartitionTable(3).serialize();

  private TestUtils() {
    // util class
  }

  public static Node getNode(int nodeNum) {
    Node node = new Node();
    node.setInternalIp("192.168.0." + nodeNum);
    node.setMetaPort(ClusterDescriptor.getInstance().getConfig().getInternalMetaPort());
    node.setDataPort(ClusterDescriptor.getInstance().getConfig().getInternalDataPort());
    node.setNodeIdentifier(nodeNum);
    node.setClientPort(IoTDBDescriptor.getInstance().getConfig().getRpcPort());
    node.setClientIp(IoTDBDescriptor.getInstance().getConfig().getRpcAddress());
    return node;
  }

  public static RaftNode getRaftNode(int nodeNum, int raftId) {
    return new RaftNode(getNode(nodeNum), raftId);
  }

  public static List<Log> prepareNodeLogs(int logNum) {
    List<Log> logList = new ArrayList<>();
    for (int i = 0; i < logNum; i++) {
      AddNodeLog log = new AddNodeLog();
      log.setNewNode(getNode(i));
      log.setPartitionTable(seralizePartitionTable);
      log.setCurrLogIndex(i);
      log.setCurrLogTerm(i);
      logList.add(log);
    }
    return logList;
  }

  public static StartUpStatus getStartUpStatus() {
    StartUpStatus startUpStatus = new StartUpStatus();
    startUpStatus.setPartitionInterval(
        IoTDBDescriptor.getInstance().getConfig().getPartitionInterval());
    startUpStatus.setHashSalt(ClusterConstant.HASH_SALT);
    startUpStatus.setReplicationNumber(
        ClusterDescriptor.getInstance().getConfig().getReplicationNum());
    startUpStatus.setClusterName(ClusterDescriptor.getInstance().getConfig().getClusterName());
    startUpStatus.setMultiRaftFactor(
        ClusterDescriptor.getInstance().getConfig().getMultiRaftFactor());
    List<Node> seedNodeList = new ArrayList<>();
    for (int i = 0; i < 100; i += 10) {
      seedNodeList.add(getNode(i));
    }
    startUpStatus.setSeedNodeList(seedNodeList);
    return startUpStatus;
  }

  public static List<Log> prepareTestLogs(int logNum) {
    List<Log> logList = new ArrayList<>();
    for (int i = 0; i < logNum; i++) {
      Log log = new EmptyContentLog();
      log.setCurrLogIndex(i);
      log.setCurrLogTerm(i);
      logList.add(log);
    }
    return logList;
  }

  public static List<Log> prepareLargeTestLogs(int logNum) {
    List<Log> logList = new ArrayList<>();
    for (int i = 0; i < logNum; i++) {
      Log log = new LargeTestLog();
      log.setCurrLogIndex(i);
      log.setCurrLogTerm(i);
      log.setByteSize(8192);
      logList.add(log);
    }
    return logList;
  }

  public static List<TimeValuePair> getTestTimeValuePairs(
      int offset, int size, int step, TSDataType dataType) {
    List<TimeValuePair> ret = new ArrayList<>(size);
    long currTime = offset;
    for (int i = 0; i < size; i++) {
      TsPrimitiveType value = TsPrimitiveType.getByType(dataType, currTime);
      TimeValuePair pair = new TimeValuePair(currTime, value);
      currTime += step;
      ret.add(pair);
    }
    return ret;
  }

  public static List<BatchData> getTestBatches(
      int offset, int size, int batchSize, int step, TSDataType dataType) {
    List<BatchData> ret = new ArrayList<>(size);
    long currTime = offset;
    BatchData currBatch = null;
    for (int i = 0; i < size; i++) {
      if (i % batchSize == 0) {
        if (currBatch != null) {
          ret.add(currBatch);
        }
        currBatch = new BatchData(dataType);
      }
      TsPrimitiveType value = TsPrimitiveType.getByType(dataType, currTime);
      currBatch.putAnObject(currTime, value.getValue());
      currTime += step;
    }
    if (currBatch != null) {
      ret.add(currBatch);
    }
    return ret;
  }

  public static PartitionTable getPartitionTable(int nodeNum) {
    List<Node> nodes = new ArrayList<>();
    for (int i = 0; i < nodeNum; i++) {
      nodes.add(getNode(i));
    }
    return new SlotPartitionTable(nodes, getNode(0));
  }

  public static String getTestSg(int i) {
    return "root.test" + i;
  }

  public static String getTestSeries(int sgNum, int seriesNum) {
    return getTestSg(sgNum) + "." + getTestMeasurement(seriesNum);
  }

  public static String getTestMeasurement(int seriesNum) {
    return "s" + seriesNum;
  }

  public static IMeasurementSchema getTestMeasurementSchema(int seriesNum) {
    TSDataType dataType = TSDataType.DOUBLE;
    TSEncoding encoding = IoTDBDescriptor.getInstance().getConfig().getDefaultDoubleEncoding();
    return new UnaryMeasurementSchema(
        TestUtils.getTestMeasurement(seriesNum),
        dataType,
        encoding,
        CompressionType.UNCOMPRESSED,
        Collections.emptyMap());
  }

  public static IMeasurementMNode getTestMeasurementMNode(int seriesNum) {
    TSDataType dataType = TSDataType.DOUBLE;
    TSEncoding encoding = IoTDBDescriptor.getInstance().getConfig().getDefaultDoubleEncoding();
    IMeasurementSchema measurementSchema =
        new UnaryMeasurementSchema(
            TestUtils.getTestMeasurement(seriesNum),
            dataType,
            encoding,
            CompressionType.UNCOMPRESSED,
            Collections.emptyMap());
    return MeasurementMNode.getMeasurementMNode(
        null, measurementSchema.getMeasurementId(), measurementSchema, null);
  }

  public static TimeseriesSchema getTestTimeSeriesSchema(int sgNum, int seriesNum) {
    TSDataType dataType = TSDataType.DOUBLE;
    TSEncoding encoding = IoTDBDescriptor.getInstance().getConfig().getDefaultDoubleEncoding();
    return new TimeseriesSchema(
        TestUtils.getTestSeries(sgNum, seriesNum),
        dataType,
        encoding,
        CompressionType.UNCOMPRESSED,
        Collections.emptyMap());
  }

  public static BatchData genBatchData(TSDataType dataType, int offset, int size) {
    BatchData batchData = new BatchData(dataType);
    for (long i = offset; i < offset + size; i++) {
      switch (dataType) {
        case DOUBLE:
          batchData.putDouble(i, i * 1.0);
          break;
        case TEXT:
          batchData.putBinary(i, new Binary(String.valueOf(i)));
          break;
        case INT64:
          batchData.putLong(i, i);
          break;
        case INT32:
          batchData.putInt(i, (int) i);
          break;
        case FLOAT:
          batchData.putFloat(i, i * 1.0f);
          break;
        case BOOLEAN:
          batchData.putBoolean(i, (i % 2) == 1);
          break;
      }
    }
    return batchData;
  }

  public static boolean batchEquals(BatchData batchA, BatchData batchB) {
    if (batchA == batchB) {
      return true;
    }
    if (batchA == null || batchB == null) {
      return false;
    }
    if (!batchA.getDataType().equals(batchB.getDataType())) {
      return false;
    }
    if (batchA.length() != batchB.length()) {
      return false;
    }
    while (batchA.hasCurrent()) {
      if (!batchB.hasCurrent()) {
        return false;
      }
      long timeA = batchA.currentTime();
      Object valueA = batchA.currentValue();
      long timeB = batchB.currentTime();
      Object valueB = batchB.currentValue();
      if (timeA != timeB || !valueA.equals(valueB)) {
        return false;
      }
      batchA.next();
      batchB.next();
    }
    return true;
  }

  public static void prepareData()
      throws QueryProcessException, StorageGroupNotSetException, StorageEngineException,
          IllegalPathException {
    InsertRowPlan insertPlan = new InsertRowPlan();
    // data for raw data query and aggregation
    // 10 devices (storage groups)
    for (int j = 0; j < 10; j++) {
      insertPlan.setPrefixPath(new PartialPath(getTestSg(j)));
      String[] measurements = new String[10];
      IMeasurementMNode[] mNodes = new IMeasurementMNode[10];
      // 10 series each device, all double
      for (int i = 0; i < 10; i++) {
        measurements[i] = getTestMeasurement(i);
        mNodes[i] = TestUtils.getTestMeasurementMNode(i);
      }
      insertPlan.setMeasurements(measurements);
      insertPlan.setNeedInferType(true);
      insertPlan.setDataTypes(new TSDataType[insertPlan.getMeasurements().length]);
      // the first sequential file
      for (int i = 10; i < 20; i++) {
        insertPlan.setTime(i);
        Object[] values = new Object[10];
        for (int k = 0; k < 10; k++) {
          values[k] = String.valueOf(i);
        }
        insertPlan.setValues(values);
        insertPlan.setMeasurementMNodes(mNodes);
        PlanExecutor planExecutor = new PlanExecutor();
        planExecutor.processNonQuery(insertPlan);
      }
      StorageEngine.getInstance().syncCloseAllProcessor();
      // the first unsequential file, not overlapped with the sequential file
      for (int i = 0; i < 10; i++) {
        insertPlan.setTime(i);
        Object[] values = new Object[10];
        for (int k = 0; k < 10; k++) {
          values[k] = String.valueOf(i);
        }
        insertPlan.setValues(values);
        insertPlan.setMeasurementMNodes(mNodes);
        PlanExecutor planExecutor = new PlanExecutor();
        planExecutor.processNonQuery(insertPlan);
      }
      StorageEngine.getInstance().syncCloseAllProcessor();
      // the second unsequential file, overlapped with the sequential file
      for (int i = 10; i < 20; i++) {
        insertPlan.setTime(i);
        Object[] values = new Object[10];
        for (int k = 0; k < 10; k++) {
          values[k] = String.valueOf(i);
        }
        insertPlan.setValues(values);
        insertPlan.setMeasurementMNodes(mNodes);
        PlanExecutor planExecutor = new PlanExecutor();
        planExecutor.processNonQuery(insertPlan);
      }
      StorageEngine.getInstance().syncCloseAllProcessor();
    }

    // data for fill
    insertPlan.setPrefixPath(new PartialPath(getTestSg(0)));
    String[] measurements = new String[] {getTestMeasurement(10)};
    IMeasurementMNode[] schemas = new IMeasurementMNode[] {TestUtils.getTestMeasurementMNode(10)};
    insertPlan.setMeasurements(measurements);
    insertPlan.setNeedInferType(true);
    insertPlan.setDataTypes(new TSDataType[insertPlan.getMeasurements().length]);
    for (int i : new int[] {0, 10}) {
      insertPlan.setTime(i);
      Object[] values = new Object[] {String.valueOf(i)};
      insertPlan.setValues(values);
      insertPlan.setMeasurementMNodes(schemas);
      PlanExecutor planExecutor = new PlanExecutor();
      planExecutor.processNonQuery(insertPlan);
    }
  }

  /**
   * The TsFileResource's path should be consist with the {@link
   * org.apache.iotdb.tsfile.utils.FilePathUtils#splitTsFilePath(String)}
   */
  public static List<TsFileResource> prepareTsFileResources(
      int sgNum, int fileNum, int seriesNum, int ptNum, boolean asHardLink)
      throws IOException, WriteProcessException {
    List<TsFileResource> ret = new ArrayList<>();
    for (int i = 0; i < fileNum; i++) {
      String fileName =
          "target"
              + File.separator
              + "data"
              + File.separator
              + String.format(
                  TestUtils.getTestSg(sgNum)
                      + File.separator
                      + 0
                      + File.separator
                      + 0
                      + File.separator
                      + "0-%d-0-0"
                      + TsFileConstant.TSFILE_SUFFIX,
                  i);
      if (asHardLink) {
        fileName = fileName + ".0_0";
      }
      File file = new File(fileName);
      file.getParentFile().mkdirs();
      try (TsFileWriter writer = new TsFileWriter(file)) {
        for (int k = 0; k < seriesNum; k++) {
          IMeasurementSchema schema = getTestMeasurementSchema(k);
          writer.registerTimeseries(new Path(getTestSg(sgNum), schema.getMeasurementId()), schema);
        }

        for (int j = 0; j < ptNum; j++) {
          long timestamp = i * ptNum + j;
          TSRecord record = new TSRecord(timestamp, getTestSg(sgNum));
          for (int k = 0; k < seriesNum; k++) {
            IMeasurementSchema schema = getTestMeasurementSchema(k);
            DataPoint dataPoint =
                DataPoint.getDataPoint(
                    schema.getType(), schema.getMeasurementId(), String.valueOf(k));
            record.addTuple(dataPoint);
          }
          writer.write(record);
        }
      }

      TsFileResource resource = new TsFileResource(file);
      resource.updateStartTime(TestUtils.getTestSg(sgNum), i * ptNum);
      resource.updateEndTime(TestUtils.getTestSg(sgNum), (i + 1) * ptNum - 1);
      resource.setMaxPlanIndex(i);
      resource.setMinPlanIndex(i);

      resource.serialize();
      ret.add(resource);
    }
    return ret;
  }
}
