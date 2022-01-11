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

package org.apache.iotdb.db.engine.compaction.inner;

import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.db.engine.cache.TimeSeriesMetadataCache;
import org.apache.iotdb.db.engine.compaction.TestUtilsForAlignedSeries;
import org.apache.iotdb.db.engine.compaction.inner.utils.InnerSpaceCompactionUtils;
import org.apache.iotdb.db.engine.compaction.utils.CompactionCheckerUtils;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class InnerSpaceCompactionUtilsAlignedTest {
  private static final String storageGroup = "root.testAlignedCompaction";
  private static File dataDirectory =
      new File(
          TestConstant.BASE_OUTPUT_PATH
              + "sequence".concat(File.separator)
              + storageGroup.concat(File.separator)
              + "0".concat(File.separator)
              + "0".concat(File.separator));

  @Before
  public void setUp() throws Exception {
    if (!dataDirectory.exists()) {
      Assert.assertTrue(dataDirectory.mkdirs());
    }
    IoTDB.metaManager.init();
  }

  @After
  public void tearDown() throws Exception {
    FileUtils.forceDelete(dataDirectory);
    IoTDB.metaManager.clear();
    ChunkCache.getInstance().clear();
    TimeSeriesMetadataCache.getInstance().clear();
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testSimpleAlignedTsFileCompaction() throws Exception {
    List<String> devices = new ArrayList<>();
    for (int i = 0; i < 5; ++i) {
      devices.add(storageGroup + ".d" + i);
    }
    boolean[] aligned = new boolean[] {true, true, true, true, true};
    List<MeasurementSchema> schemas = new ArrayList<>();
    schemas.add(new MeasurementSchema("s0", TSDataType.DOUBLE));
    schemas.add(new MeasurementSchema("s1", TSDataType.FLOAT));
    schemas.add(new MeasurementSchema("s2", TSDataType.INT64));
    schemas.add(new MeasurementSchema("s3", TSDataType.INT32));
    schemas.add(new MeasurementSchema("s4", TSDataType.TEXT));
    schemas.add(new MeasurementSchema("s5", TSDataType.BOOLEAN));

    TestUtilsForAlignedSeries.registerTimeSeries(
        storageGroup,
        devices.toArray(new String[] {}),
        schemas.toArray(new MeasurementSchema[] {}),
        aligned);

    boolean[] randomNull = new boolean[] {false, false, false, false, false};
    int timeInterval = 500;
    List<TsFileResource> resources = new ArrayList<>();
    for (int i = 0; i < 30; i++) {
      TsFileResource resource =
          new TsFileResource(new File(dataDirectory, String.format("%d-%d-0-0.tsfile", i, i)));
      TestUtilsForAlignedSeries.writeTsFile(
          devices.toArray(new String[] {}),
          schemas.toArray(new MeasurementSchema[0]),
          resource,
          aligned,
          timeInterval * i,
          timeInterval * (i + 1),
          randomNull);
      resources.add(resource);
    }
    TsFileResource targetResource = new TsFileResource(new File(dataDirectory, "1-1-1-0.tsfile"));
    List<String> fullPaths = new ArrayList<>();
    List<IMeasurementSchema> iMeasurementSchemas = new ArrayList<>();
    for (String device : devices) {
      for (MeasurementSchema schema : schemas) {
        iMeasurementSchemas.add(schema);
        fullPaths.add(device + "." + schema.getMeasurementId());
      }
    }
    Map<String, List<TimeValuePair>> originData =
        CompactionCheckerUtils.getDataByQuery(
            fullPaths, iMeasurementSchemas, resources, new ArrayList<>());
    InnerSpaceCompactionUtils.compact(targetResource, resources, true);
    Map<String, List<TimeValuePair>> compactedData =
        CompactionCheckerUtils.getDataByQuery(
            fullPaths,
            iMeasurementSchemas,
            Collections.singletonList(targetResource),
            new ArrayList<>());
    CompactionCheckerUtils.validDataByValueList(originData, compactedData);
  }
}
