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

package org.apache.iotdb.db.engine.merge;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MergeUnseqChunkReaderTest extends MergeTest {

  private File tempSGDir;

  @Override
  @Before
  public void setUp()
      throws IOException, WriteProcessException, MetadataException, MetadataException {
    super.setUp();
    File file =
        new File(
            TestConstant.BASE_OUTPUT_PATH.concat(
                unseqFileNum
                    + "unseq"
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + unseqFileNum
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + unseqFileNum
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + ".tsfile"));
    TsFileResource tsFileResource = new TsFileResource(file);
    tsFileResource.setClosed(true);
    tsFileResource.setMinPlanIndex(unseqFileNum + seqFileNum);
    tsFileResource.setMaxPlanIndex(unseqFileNum + seqFileNum);
    tsFileResource.setVersion(unseqFileNum + seqFileNum);
    unseqResources.add(tsFileResource);
    TsFileWriter fileWriter = new TsFileWriter(tsFileResource.getTsFile());
    fileWriter.registerTimeseries(
        new Path(deviceIds[0], measurementSchemas[0].getMeasurementId()), measurementSchemas[0]);
    for (long i = 50; i < 100; i++) {
      TSRecord record = new TSRecord(i, deviceIds[0]);
      record.addTuple(
          DataPoint.getDataPoint(
              measurementSchemas[0].getType(),
              measurementSchemas[0].getMeasurementId(),
              String.valueOf(i + 10000)));
      fileWriter.write(record);
      tsFileResource.updateStartTime(deviceIds[0], i);
      tsFileResource.updateEndTime(deviceIds[0], i);
    }
    for (long i = 0; i < 50; i++) {
      TSRecord record = new TSRecord(i, deviceIds[0]);
      record.addTuple(
          DataPoint.getDataPoint(
              measurementSchemas[0].getType(),
              measurementSchemas[0].getMeasurementId(),
              String.valueOf(i + 10000)));
      fileWriter.write(record);
      tsFileResource.updateStartTime(deviceIds[0], i);
      tsFileResource.updateEndTime(deviceIds[0], i);
    }
    fileWriter.flushAllChunkGroups();
    //        prepareFile(tsFileResource, 100, ptNum, 10000);
    //        prepareFile(tsFileResource, 0, ptNum, 10000);
    tempSGDir = new File(TestConstant.BASE_OUTPUT_PATH.concat("tempSG"));
    tempSGDir.mkdirs();
  }

  @Override
  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
    FileUtils.deleteDirectory(tempSGDir);
  }

  @Test
  public void testMergeUnseqChunkReader() throws Exception {
    MergeResource mergeResource =
        new MergeResource(seqResources, unseqResources.subList(2, unseqResources.size() - 1));
    PartialPath path = new PartialPath("root.mergeTest.device0", "sensor0");
    Map<PartialPath, MeasurementSchema> measurementSchemaMap = new HashMap<>();
    measurementSchemaMap.put(path, measurementSchemas[0]);
    mergeResource.setMeasurementSchemaMap(measurementSchemaMap);
    List<PartialPath> partialPathList = new ArrayList<>();
    partialPathList.add(path);
    IPointReader pointReader = mergeResource.getUnseqReaders(partialPathList)[0];
    while (pointReader.hasNextTimeValuePair()) {
      System.out.println(pointReader.nextTimeValuePair().getTimestamp());
    }
  }
}
