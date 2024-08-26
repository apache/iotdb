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

package org.apache.iotdb.db.storageengine.dataregion.compaction.settle;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSettleReq;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.DataRegionException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.TsFileProcessorException;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.DataRegionTest;
import org.apache.iotdb.db.storageengine.dataregion.flush.FlushManager;
import org.apache.iotdb.db.storageengine.dataregion.flush.TsFileFlushPolicy;
import org.apache.iotdb.db.storageengine.dataregion.memtable.TsFileProcessor;
import org.apache.iotdb.db.storageengine.dataregion.wal.WALManager;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.constant.TestConstant;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.record.datapoint.DataPoint;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SettleRequestHandlerTest {
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private SettleRequestHandler reqHandler;
  private String storageGroup = "root.sg.d1";
  private String systemDir = TestConstant.OUTPUT_DATA_DIR.concat("info");
  private String deviceId = "root.sg.d1";
  private String measurementId = "s0";
  private List<String> paths = new ArrayList<>();
  private DataRegion dataRegion;

  @Before
  public void setUp()
      throws DataRegionException, StartupException, IOException, StorageEngineException {
    EnvironmentUtils.cleanDir(TestConstant.OUTPUT_DATA_DIR);
    EnvironmentUtils.cleanEnv();
    EnvironmentUtils.envSetUp();
    reqHandler = SettleRequestHandler.getInstance();
    reqHandler.setTestMode(true);
    dataRegion = new DummyDataRegion(systemDir, storageGroup);
    StorageEngine.getInstance().setDataRegion(new DataRegionId(0), dataRegion);
    WALManager.getInstance().start();
    FlushManager.getInstance().start();
  }

  @After
  public void tearDown() throws StorageEngineException, IOException, TsFileProcessorException {
    WALManager.getInstance().stop();
    FlushManager.getInstance().stop();
    if (dataRegion != null) {
      dataRegion.syncDeleteDataFiles();
      StorageEngine.getInstance().deleteDataRegion(new DataRegionId(0));
    }
    EnvironmentUtils.cleanEnv();
    EnvironmentUtils.cleanDir(TestConstant.OUTPUT_DATA_DIR);
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testHandleSettleRequest()
      throws IllegalPathException, IOException, WriteProcessException {
    createTsFiles();
    Assert.assertEquals(3, paths.size());

    // 3 TsFile compaction
    TSettleReq req = new TSettleReq();
    req.setPaths(paths);
    TSStatus result = reqHandler.handleSettleRequest(req);
    Assert.assertEquals(result.code, TSStatusCode.SUCCESS_STATUS.getStatusCode());

    // not enable compaction
    config.setEnableSeqSpaceCompaction(false);
    result = reqHandler.handleSettleRequest(req);
    Assert.assertEquals(result.code, TSStatusCode.UNSUPPORTED_OPERATION.getStatusCode());
    config.setEnableSeqSpaceCompaction(true);

    // compaction candidate file num
    int maxInnerCompactionCandidateFileNum = config.getInnerCompactionCandidateFileNum();
    config.setInnerCompactionCandidateFileNum(2);
    result = reqHandler.handleSettleRequest(req);
    Assert.assertEquals(result.code, TSStatusCode.UNSUPPORTED_OPERATION.getStatusCode());
    String firstTsFilePath = paths.remove(0);
    result = reqHandler.handleSettleRequest(req);
    Assert.assertEquals(result.code, TSStatusCode.SUCCESS_STATUS.getStatusCode());
    paths.add(0, firstTsFilePath);
    config.setInnerCompactionCandidateFileNum(maxInnerCompactionCandidateFileNum);

    // not continuous
    paths.remove(1);
    result = reqHandler.handleSettleRequest(req);
    Assert.assertEquals(result.code, TSStatusCode.ILLEGAL_PARAMETER.getStatusCode());

    // mods file not exist
    paths.remove(0);
    result = reqHandler.handleSettleRequest(req);
    Assert.assertEquals(result.code, TSStatusCode.ILLEGAL_PARAMETER.getStatusCode());
  }

  private void createTsFiles() throws IllegalPathException, WriteProcessException, IOException {
    TSRecord record;
    for (int i = 0; i < 3; i++) {
      for (int j = 1; j <= 3; j++) {
        long timestamp = 3L * i + j;
        record = new TSRecord(timestamp, deviceId);
        record.addTuple(
            DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(timestamp)));
        dataRegion.insert(DataRegionTest.buildInsertRowNodeByTSRecord(record));
      }
      for (TsFileProcessor tsFileProcessor : dataRegion.getWorkSequenceTsFileProcessors()) {
        paths.add(tsFileProcessor.getTsFileResource().getTsFilePath());
      }
      dataRegion.syncCloseAllWorkingTsFileProcessors();
      if (i != 2) {
        dataRegion.deleteByDevice(
            new MeasurementPath(deviceId, measurementId), 3L * i + 1, 3L * i + 1, -1);
      }
    }
  }

  static class DummyDataRegion extends DataRegion {

    DummyDataRegion(String systemInfoDir, String storageGroupName) throws DataRegionException {
      super(systemInfoDir, "0", new TsFileFlushPolicy.DirectFlushPolicy(), storageGroupName);
    }
  }
}
