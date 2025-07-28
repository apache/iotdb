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

package org.apache.iotdb.db.storageengine.dataregion.compaction.inner;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.constant.CompactionTaskType;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.ICompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.ReadChunkCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.CompactionTaskSummary;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.CompactionUtils;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.CompactionLogger;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.utils.constant.TestConstant;

import org.apache.commons.io.FileUtils;
import org.apache.tsfile.read.TsFileReader;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.query.dataset.QueryDataSet;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * This test is used for old version of inner space compact. We leave it as it still validates the
 * current compaction. However, due to this test's strong coupling with an older version of
 * compaction, we may remove it in the future.
 */
public class ReadChunkCompactionPerformerOldTest extends InnerCompactionTest {

  File tempSGDir;

  @Override
  @Before
  public void setUp() throws Exception {
    tempSGDir = new File(TestConstant.getTestTsFileDir("root.compactionTest", 0, 0));
    if (!tempSGDir.exists()) {
      assertTrue(tempSGDir.mkdirs());
    }
    super.setUp();
  }

  @Override
  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
    FileUtils.deleteDirectory(new File("target/testTsFile"));
  }

  @Test
  public void testCompact() throws Exception {
    TsFileResource targetTsFileResource =
        new TsFileResource(
            new File(
                TestConstant.getTestTsFileDir("root.compactionTest", 0, 0)
                    .concat(
                        0
                            + IoTDBConstant.FILE_NAME_SEPARATOR
                            + 0
                            + IoTDBConstant.FILE_NAME_SEPARATOR
                            + 1
                            + IoTDBConstant.FILE_NAME_SEPARATOR
                            + 0
                            + IoTDBConstant.INNER_COMPACTION_TMP_FILE_SUFFIX)));
    File targetFile =
        new File(
            TestConstant.getTestTsFileDir("root.compactionTest", 0, 0)
                .concat(
                    0
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + 0
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + 1
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + 0
                        + ".tsfile"));
    if (targetFile.exists()) {
      assertTrue(targetFile.delete());
    }
    CompactionLogger sizeTieredCompactionLogger =
        new CompactionLogger(
            new File(targetTsFileResource.getTsFilePath().concat(".compaction.log")));
    sizeTieredCompactionLogger.logFiles(seqResources, CompactionLogger.STR_SOURCE_FILES);
    ICompactionPerformer performer =
        new ReadChunkCompactionPerformer(seqResources, targetTsFileResource);
    performer.setSummary(new CompactionTaskSummary());
    performer.perform();
    CompactionUtils.moveTargetFile(
        Collections.singletonList(targetTsFileResource),
        CompactionTaskType.INNER_SEQ,
        COMPACTION_TEST_SG);
    sizeTieredCompactionLogger.close();
    Path path = new Path(deviceIds[0], measurementSchemas[0].getMeasurementName(), true);
    try (TsFileSequenceReader reader =
            new TsFileSequenceReader(targetTsFileResource.getTsFilePath());
        TsFileReader readTsFile = new TsFileReader(reader)) {
      QueryExpression queryExpression =
          QueryExpression.create(Collections.singletonList(path), null);
      QueryDataSet queryDataSet = readTsFile.query(queryExpression);
      int cut = 0;
      RowRecord record;
      while (queryDataSet.hasNext()) {
        record = queryDataSet.next();
        assertEquals(record.getTimestamp(), record.getFields().get(0).getDoubleV(), 0.001);
        cut++;
      }
      assertEquals(600, cut);
    }
  }
}
