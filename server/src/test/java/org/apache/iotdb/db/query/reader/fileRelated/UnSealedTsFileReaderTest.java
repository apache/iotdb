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

package org.apache.iotdb.db.query.reader.fileRelated;

import java.io.IOException;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.qp.QueryProcessorException;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.reader.ReaderTestHelper;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.junit.Assert;
import org.junit.Test;

public class UnSealedTsFileReaderTest extends ReaderTestHelper {

  private QueryContext context = EnvironmentUtils.TEST_QUERY_CONTEXT;

  @Test
  public void testUnSealedTsFileIterateReader() throws IOException {
    QueryDataSource queryDataSource = storageGroupProcessor.query(deviceId, measurementId, context,
        null);
    TsFileResource resource = queryDataSource.getSeqResources().get(0);
    Assert.assertFalse(resource.isClosed());
    UnSealedTsFileIterateReader reader = new UnSealedTsFileIterateReader(resource, null, false);
    long time = 999;
    while (reader.hasNext()) {
      BatchData batchData = reader.nextBatch();
      while (batchData.hasNext()) {
        time++;
        Assert.assertEquals(time, batchData.currentTime());
        batchData.next();
      }
    }
    Assert.assertEquals(3029L, time);
  }

  @Test
  public void testUnSealedTsFileReaderByTimestamp() throws IOException {
    QueryDataSource queryDataSource = storageGroupProcessor.query(deviceId, measurementId, context,
        null);
    TsFileResource resource = queryDataSource.getSeqResources().get(0);
    Assert.assertFalse(resource.isClosed());
    UnSealedTsFileReaderByTimestamp reader = new UnSealedTsFileReaderByTimestamp(
        resource);

    // unSealedTsFileDiskReaderByTs
    for (int time = 1000; time <= 3019; time += 10) {
      int value = (int) reader.getValueInTimestamp(time);
      Assert.assertEquals(time, value);
    }

    // unSealedTsFileMemReaderByTs
    for (int time = 3020; time <= 3029; time += 5) {
      int value = (int) reader.getValueInTimestamp(time);
      Assert.assertEquals(time, value);

    }
    Assert.assertTrue(reader.hasNext());

    for (int time = 3050; time <= 3080; time += 10) {
      Integer value = (Integer) reader.getValueInTimestamp(time);
      Assert.assertNull(value);
    }
    Assert.assertFalse(reader.hasNext());
  }


  @Override
  protected void insertData() throws IOException, QueryProcessorException {
    for (int j = 1000; j <= 1009; j++) {
      insertOneRecord(j, j);
    }
    storageGroupProcessor.getWorkSequenceTsFileProcessor().syncFlush();
    for (int j = 1010; j <= 1019; j++) {
      insertOneRecord(j, j);
    }
    storageGroupProcessor.getWorkSequenceTsFileProcessor().syncFlush();
    for (int j = 1020; j <= 3019; j++) {
      insertOneRecord(j, j);
    }
    storageGroupProcessor.getWorkSequenceTsFileProcessor().syncFlush();
    for (int j = 3020; j <= 3029; j = j + 1) {
      insertOneRecord(j, j);
    }
  }
}