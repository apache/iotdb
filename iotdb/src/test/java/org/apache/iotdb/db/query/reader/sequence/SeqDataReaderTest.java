/**
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
package org.apache.iotdb.db.query.reader.sequence;

import java.io.IOException;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.StorageGroupProcessorException;
import org.apache.iotdb.db.query.reader.ReaderTestHelper;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.junit.Assert;
import org.junit.Test;

public class SeqDataReaderTest extends ReaderTestHelper {

  @Test
  public void testSeqReader() throws IOException, StorageGroupProcessorException {
    QueryDataSource queryDataSource = storageGroupProcessor.query(deviceId, measurementId);
    Path path = new Path(deviceId, measurementId);
    SequenceSeriesReader readerV2 = new SequenceSeriesReader(path,
        queryDataSource.getSeqResources(), null,
        EnvironmentUtils.TEST_QUERY_CONTEXT);
    long time = 999;
    while (readerV2.hasNext()) {
      BatchData batchData = readerV2.nextBatch();
      while (batchData.hasNext()) {
        time++;
        Assert.assertEquals(time, batchData.currentTime());
        batchData.next();
      }
    }
    Assert.assertEquals(3029L, time);
  }

  @Test
  public void testSeqByTimestampReader() throws IOException, StorageGroupProcessorException {
    QueryDataSource queryDataSource = storageGroupProcessor.query(deviceId, measurementId);
    Path path = new Path(deviceId, measurementId);
    SequenceSeriesReaderByTimestamp readerV2 = new SequenceSeriesReaderByTimestamp(path,
        queryDataSource.getSeqResources(), EnvironmentUtils.TEST_QUERY_CONTEXT);

    for (int time = 1000; time <= 3020; time += 10) {
      int value = (int) readerV2.getValueInTimestamp(time);
      Assert.assertEquals(time, value);
    }

    Assert.assertEquals(true, readerV2.hasNext());
    for (int time = 3050; time <= 3080; time += 10) {
      Integer value = (Integer) readerV2.getValueInTimestamp(time);
      Assert.assertEquals(null, value);
    }
    Assert.assertEquals(false, readerV2.hasNext());
  }


  @Override
  protected void insertData() {
    for (int j = 1000; j <= 1009; j++) {
      insertOneRecord(j, j);
      storageGroupProcessor.asyncForceClose();
    }
    for (int j = 1010; j <= 1019; j++) {
      insertOneRecord(j, j);
      storageGroupProcessor.getWorkSequenceTsFileProcessor().syncFlush();
    }
    storageGroupProcessor.syncCloseFileNode();

    for (int j = 1020; j <= 3019; j++) {
      insertOneRecord(j, j);
    }
    storageGroupProcessor.syncCloseFileNode();

    assert storageGroupProcessor.getWorkSequenceTsFileProcessor() == null;

    for (int j = 3020; j <= 3029; j++) {
      insertOneRecord(j, j);
    }
  }
}