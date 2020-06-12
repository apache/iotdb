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

package org.apache.iotdb.db.engine.merge.seqMerge.squeeze;

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
import org.apache.iotdb.db.engine.merge.seqMerge.squeeze.task.SqueezeMergeTask;
import org.apache.iotdb.db.engine.merge.sizeMerge.MergeSizeSelectorStrategy;
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

public class SqueezeMergeTaskTest extends MergeTest {

  private File tempSGDir;
  private MergeSizeSelectorStrategy preMergeSizeSelectorStrategy;

  @Before
  public void setUp() throws IOException, WriteProcessException, MetadataException {
    super.setUp();
    preMergeSizeSelectorStrategy = IoTDBDescriptor.getInstance().getConfig()
        .getMergeSizeSelectorStrategy();
    IoTDBDescriptor.getInstance().getConfig().setMergeSizeSelectorStrategy(
        MergeSizeSelectorStrategy.TIME_RANGE);
    tempSGDir = new File(TestConstant.BASE_OUTPUT_PATH.concat("tempSG"));
    tempSGDir.mkdirs();
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
    IoTDBDescriptor.getInstance().getConfig().setMergeSizeSelectorStrategy(
        preMergeSizeSelectorStrategy);
    FileUtils.deleteDirectory(tempSGDir);
  }

  @Test
  public void testMerge() throws Exception {
    TsFileResource[] newResource = new TsFileResource[1];
    SqueezeMergeTask mergeTask =
        new SqueezeMergeTask(new MergeResource(seqResources, unseqResources), tempSGDir.getPath(),
            (k, v
                , l, n) -> newResource[0] = n.get(0), "test", MERGE_TEST_SG);
    mergeTask.call();
    newResource[0].close();

    QueryContext context = new QueryContext();
    Path path = new Path(deviceIds[0], measurementSchemas[0].getMeasurementId());
    List<TsFileResource> list = new ArrayList<>();
    list.add(newResource[0]);
    IBatchReader tsFilesReader = new SeriesRawDataBatchReader(path, measurementSchemas[0].getType(),
        context,
        list, new ArrayList<>(), null, null);
    int cnt = 0;
    while (tsFilesReader.hasNextBatch()) {
      BatchData batchData = tsFilesReader.nextBatch();
      for (int i = 0; i < batchData.length(); i++) {
        assertEquals(batchData.getTimeByIndex(i) + 20000.0, batchData.getDoubleByIndex(i), 0.001);
        cnt++;
      }
    }
    assertEquals(500, cnt);
    tsFilesReader.close();
    newResource[0].remove();
  }

  @Test
  public void testChunkNumThreshold() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setChunkMergePointThreshold(Integer.MAX_VALUE);
    TsFileResource[] newResource = new TsFileResource[1];
    SqueezeMergeTask mergeTask =
        new SqueezeMergeTask(new MergeResource(seqResources, unseqResources), tempSGDir.getPath()
            , (k, v, l, n) -> newResource[0] = n.get(0), "test", MERGE_TEST_SG);
    mergeTask.call();
    newResource[0].close();

    QueryContext context = new QueryContext();
    Path path = new Path(deviceIds[0], measurementSchemas[0].getMeasurementId());
    List<TsFileResource> list = new ArrayList<>();
    list.add(newResource[0]);
    IBatchReader tsFilesReader = new SeriesRawDataBatchReader(path, measurementSchemas[0].getType(),
        context,
        list, new ArrayList<>(), null, null);
    int cnt = 0;
    while (tsFilesReader.hasNextBatch()) {
      BatchData batchData = tsFilesReader.nextBatch();
      for (int i = 0; i < batchData.length(); i++) {
        assertEquals(batchData.getTimeByIndex(i) + 20000.0, batchData.getDoubleByIndex(i), 0.001);
        cnt++;
      }
    }
    assertEquals(500, cnt);
    tsFilesReader.close();
    newResource[0].remove();
  }

  @Test
  public void testPartialMerge1() throws Exception {
    TsFileResource[] newResource = new TsFileResource[1];
    SqueezeMergeTask mergeTask =
        new SqueezeMergeTask(new MergeResource(seqResources, unseqResources.subList(0, 1)),
            tempSGDir.getPath(),
            (k, v, l, n) -> newResource[0] = n.get(0), "test", MERGE_TEST_SG);
    mergeTask.call();
    newResource[0].close();

    QueryContext context = new QueryContext();
    Path path = new Path(deviceIds[0], measurementSchemas[0].getMeasurementId());
    List<TsFileResource> list = new ArrayList<>();
    list.add(newResource[0]);
    IBatchReader tsFilesReader = new SeriesRawDataBatchReader(path, measurementSchemas[0].getType(),
        context,
        list, new ArrayList<>(), null, null);
    int cnt = 0;
    while (tsFilesReader.hasNextBatch()) {
      BatchData batchData = tsFilesReader.nextBatch();
      for (int i = 0; i < batchData.length(); i++) {
        if (batchData.getTimeByIndex(i) < 20) {
          assertEquals(batchData.getTimeByIndex(i) + 10000.0, batchData.getDoubleByIndex(i), 0.001);
        } else {
          assertEquals(batchData.getTimeByIndex(i) + 0.0, batchData.getDoubleByIndex(i), 0.001);
        }
        cnt++;
      }
    }
    assertEquals(500, cnt);
    tsFilesReader.close();
    newResource[0].remove();
  }

  @Test
  public void testPartialMerge2() throws Exception {
    TsFileResource[] newResource = new TsFileResource[1];
    SqueezeMergeTask mergeTask =
        new SqueezeMergeTask(new MergeResource(seqResources, unseqResources.subList(5, 6)),
            tempSGDir.getPath(),
            (k, v, l, n) -> newResource[0] = n.get(0), "test", MERGE_TEST_SG);
    mergeTask.call();
    newResource[0].close();

    QueryContext context = new QueryContext();
    Path path = new Path(deviceIds[0], measurementSchemas[0].getMeasurementId());
    List<TsFileResource> list = new ArrayList<>();
    list.add(newResource[0]);
    IBatchReader tsFilesReader = new SeriesRawDataBatchReader(path, measurementSchemas[0].getType(),
        context,
        list, new ArrayList<>(), null, null);
    int cnt = 0;
    while (tsFilesReader.hasNextBatch()) {
      BatchData batchData = tsFilesReader.nextBatch();
      for (int i = 0; i < batchData.length(); i++) {
        assertEquals(batchData.getTimeByIndex(i) + 20000.0, batchData.getDoubleByIndex(i), 0.001);
        cnt++;
      }
    }
    assertEquals(500, cnt);
    tsFilesReader.close();
    newResource[0].remove();
  }

  @Test
  public void testPartialMerge3() throws Exception {
    TsFileResource[] newResource = new TsFileResource[1];
    SqueezeMergeTask mergeTask =
        new SqueezeMergeTask(new MergeResource(seqResources, unseqResources.subList(0, 5)),
            tempSGDir.getPath(),
            (k, v, l, n) -> newResource[0] = n.get(0), "test", MERGE_TEST_SG);
    mergeTask.call();
    newResource[0].close();

    QueryContext context = new QueryContext();
    Path path = new Path(deviceIds[0], measurementSchemas[0].getMeasurementId());
    List<TsFileResource> list = new ArrayList<>();
    list.add(newResource[0]);
    IBatchReader tsFilesReader = new SeriesRawDataBatchReader(path, measurementSchemas[0].getType(),
        context,
        list, new ArrayList<>(), null, null);
    int cnt = 0;
    while (tsFilesReader.hasNextBatch()) {
      BatchData batchData = tsFilesReader.nextBatch();
      for (int i = 0; i < batchData.length(); i++) {
        long time = batchData.getTimeByIndex(i);
        boolean overflowed = time < (100 * (time / 100) + 20 * (time / 100 + 1));
        if (overflowed) {
          assertEquals(batchData.getTimeByIndex(i) + 10000.0, batchData.getDoubleByIndex(i), 0.001);
        } else {
          assertEquals(batchData.getTimeByIndex(i) + 0.0, batchData.getDoubleByIndex(i), 0.001);
        }
        cnt++;
      }
    }
    assertEquals(500, cnt);
    tsFilesReader.close();
    newResource[0].remove();
  }

  @Test
  public void mergeWithDeletionTest() throws Exception {
    TsFileResource[] newResource = new TsFileResource[1];
    seqResources.get(0).getModFile().write(new Deletion(new Path(deviceIds[0],
        measurementSchemas[0].getMeasurementId()), 10000, 49));
    seqResources.get(0).getModFile().close();

    SqueezeMergeTask mergeTask =
        new SqueezeMergeTask(new MergeResource(seqResources, unseqResources.subList(0, 1)),
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
    Path path = new Path(deviceIds[0], measurementSchemas[0].getMeasurementId());
    List<TsFileResource> list = new ArrayList<>();
    list.add(newResource[0]);
    IBatchReader tsFilesReader = new SeriesRawDataBatchReader(path, measurementSchemas[0].getType(),
        context,
        list, new ArrayList<>(), null, null);
    int count = 0;
    while (tsFilesReader.hasNextBatch()) {
      BatchData batchData = tsFilesReader.nextBatch();
      for (int i = 0; i < batchData.length(); i++) {
        if (batchData.getTimeByIndex(i) <= 20) {
          assertEquals(batchData.getTimeByIndex(i) + 10000.0, batchData.getDoubleByIndex(i), 0.001);
        } else {
          assertEquals(batchData.getTimeByIndex(i), batchData.getDoubleByIndex(i), 0.001);
        }
        count++;
      }
    }
    assertEquals(470, count);
    tsFilesReader.close();
    newResource[0].remove();
  }
}
