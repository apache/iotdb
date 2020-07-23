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

package org.apache.iotdb.db.query.reader;

import org.apache.iotdb.db.conf.adapter.ActiveTimeSeriesCounter;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.MetadataManagerHelper;
import org.apache.iotdb.db.engine.flush.TsFileFlushPolicy.DirectFlushPolicy;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;

public abstract class ReaderTestHelper {

  private String storageGroup = "root.vehicle";
  protected String deviceId = "root.vehicle.d0";
  protected String measurementId = "s0";
  protected TSDataType dataType = TSDataType.INT32;
  protected StorageGroupProcessor storageGroupProcessor;
  private String systemDir = TestConstant.OUTPUT_DATA_DIR.concat("info");

  static {
    IoTDB.metaManager.init();
  }

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
    MetadataManagerHelper.initMetadata();
    ActiveTimeSeriesCounter.getInstance().init(storageGroup);
    storageGroupProcessor = new StorageGroupProcessor(systemDir, storageGroup, new DirectFlushPolicy());
    insertData();
  }

  @After
  public void tearDown() throws Exception {
    FileReaderManager.getInstance().closeAndRemoveAllOpenedReaders();
    storageGroupProcessor.syncDeleteDataFiles();
    EnvironmentUtils.cleanEnv();
    EnvironmentUtils.cleanDir(systemDir);
  }

  abstract protected void insertData() throws IOException, QueryProcessException;

}