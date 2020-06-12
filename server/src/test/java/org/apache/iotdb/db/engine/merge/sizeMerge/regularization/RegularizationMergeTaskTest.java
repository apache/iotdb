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

package org.apache.iotdb.db.engine.merge.sizeMerge.regularization;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.merge.MergeTest;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.merge.sizeMerge.regularization.task.RegularizationMergeTask;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.reader.series.SeriesRawDataBatchReader;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class RegularizationMergeTaskTest extends MergeTest {

  private File tempSGDir;
  private int preChunkMergePointThreshold;

  @Before
  public void setUp()
      throws IOException, WriteProcessException, MetadataException {
    super.setUp();
    preChunkMergePointThreshold = IoTDBDescriptor.getInstance().getConfig()
        .getChunkMergePointThreshold();
    IoTDBDescriptor.getInstance().getConfig().setChunkMergePointThreshold(100);
    tempSGDir = new File(TestConstant.BASE_OUTPUT_PATH.concat("tempSG"));
    tempSGDir.mkdirs();
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
    IoTDBDescriptor.getInstance().getConfig()
        .setChunkMergePointThreshold(preChunkMergePointThreshold);
    FileUtils.deleteDirectory(tempSGDir);
  }

  @Test
  public void testMerge() throws Exception {
    TsFileResource[] newResource = new TsFileResource[1];
    RegularizationMergeTask mergeTask =
        new RegularizationMergeTask(new MergeResource(seqResources, unseqResources),
            tempSGDir.getPath(),
            (k, v, l, n) -> newResource[0] = n.get(0), "test", MERGE_TEST_SG);
    mergeTask.call();

    QueryContext context = new QueryContext();
    Path path = new Path(deviceIds[0], measurementSchemas[0].getMeasurementId());
    List<TsFileResource> list = new ArrayList<>();
    list.add(newResource[0]);
    IBatchReader tsFilesReader = new SeriesRawDataBatchReader(path, measurementSchemas[0].getType(),
        context, list, new ArrayList<>(), null, null);
    while (tsFilesReader.hasNextBatch()) {
      BatchData batchData = tsFilesReader.nextBatch();
      for (int i = 0; i < batchData.length(); i++) {
        assertEquals(batchData.getTimeByIndex(i) + 20000.0, batchData.getDoubleByIndex(i), 0.001);
      }
    }
    tsFilesReader.close();
    newResource[0].remove();
  }

  @Test
  public void mergeWithDeletionTest() throws Exception {
    try {
      seqResources.get(0).getModFile().write(new Deletion(new Path(deviceIds[0],
          measurementSchemas[0].getMeasurementId()), 10000, 49));
    } finally {
      seqResources.get(0).getModFile().close();
    }

    TsFileResource[] newResource = new TsFileResource[1];
    RegularizationMergeTask mergeTask =
        new RegularizationMergeTask(new MergeResource(seqResources, unseqResources.subList(0, 1)),
            tempSGDir.getPath(),
            (k, v, l, n) -> {
              try {
                seqResources.get(0).removeModFile();
              } catch (IOException e) {
                e.printStackTrace();
              }
              newResource[0] = n.get(0);
            }, "test", MERGE_TEST_SG);
    mergeTask.call();
    newResource[0].close();

    QueryContext context = new QueryContext();
    Path path = new Path(deviceIds[1], measurementSchemas[0].getMeasurementId());
    List<TsFileResource> resources = new ArrayList<>();
    resources.add(newResource[0]);
    IBatchReader tsFilesReader = new SeriesRawDataBatchReader(path, measurementSchemas[0].getType(),
        context,
        resources, new ArrayList<>(), null, null);
    int count = 0;
    while (tsFilesReader.hasNextBatch()) {
      BatchData batchData = tsFilesReader.nextBatch();
      for (int i = 0; i < batchData.length(); i++) {
        count++;
      }
    }
    assertEquals(500, count);
    tsFilesReader.close();
    newResource[0].remove();
  }
}
