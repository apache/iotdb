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
package org.apache.iotdb.db.storageengine.dataregion.tsfile;

import org.apache.iotdb.db.storageengine.dataregion.tsfile.generator.TsFileNameGenerator;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.ArrayDeviceTimeIndex;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.ITimeIndex;
import org.apache.iotdb.db.utils.constant.TestConstant;

import org.apache.commons.io.FileUtils;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

public class TsFileResourceTest {
  private final File file =
      new File(
          TsFileNameGenerator.generateNewTsFilePath(TestConstant.BASE_OUTPUT_PATH, 1, 1, 1, 1));
  private final TsFileResource tsFileResource = new TsFileResource(file);
  private final Map<IDeviceID, Integer> deviceToIndex = new HashMap<>();
  private final long[] startTimes = new long[DEVICE_NUM];
  private final long[] endTimes = new long[DEVICE_NUM];
  private static final int DEVICE_NUM = 100;

  @Before
  public void setUp() {
    IntStream.range(0, DEVICE_NUM)
        .forEach(
            i -> deviceToIndex.put(IDeviceID.Factory.DEFAULT_FACTORY.create("root.sg.d" + i), i));
    ArrayDeviceTimeIndex deviceTimeIndex =
        new ArrayDeviceTimeIndex(deviceToIndex, startTimes, endTimes);
    IntStream.range(0, DEVICE_NUM)
        .forEach(
            i -> {
              deviceTimeIndex.updateStartTime(
                  IDeviceID.Factory.DEFAULT_FACTORY.create("root.sg.d" + i), i);
              deviceTimeIndex.updateEndTime(
                  IDeviceID.Factory.DEFAULT_FACTORY.create("root.sg.d" + i), i + 1);
            });
    tsFileResource.setTimeIndex(deviceTimeIndex);
    tsFileResource.setStatusForTest(TsFileResourceStatus.NORMAL);
  }

  @After
  public void tearDown() throws IOException {
    // clean fake file
    if (file.exists()) {
      FileUtils.delete(file);
    }
    File resourceFile = new File(file.getName() + TsFileResource.RESOURCE_SUFFIX);
    if (resourceFile.exists()) {
      FileUtils.delete(resourceFile);
    }
  }

  @Test
  public void testSerializeAndDeserialize() throws IOException {
    tsFileResource.serialize();
    TsFileResource derTsFileResource = new TsFileResource(file);
    derTsFileResource.deserialize();
    Assert.assertEquals(tsFileResource, derTsFileResource);
  }

  @Test
  public void testDegradeAndFileTimeIndex() {
    Assert.assertEquals(ITimeIndex.ARRAY_DEVICE_TIME_INDEX_TYPE, tsFileResource.getTimeIndexType());
    tsFileResource.degradeTimeIndex();
    Assert.assertEquals(ITimeIndex.FILE_TIME_INDEX_TYPE, tsFileResource.getTimeIndexType());
    Assert.assertEquals(deviceToIndex.keySet(), tsFileResource.getDevices());
    for (int i = 0; i < DEVICE_NUM; i++) {
      Assert.assertEquals(
          tsFileResource.getStartTime(IDeviceID.Factory.DEFAULT_FACTORY.create("root.sg1.d" + i)),
          0);
      Assert.assertEquals(
          tsFileResource.getEndTime(IDeviceID.Factory.DEFAULT_FACTORY.create("root.sg1.d" + i)),
          DEVICE_NUM);
    }
  }
}
