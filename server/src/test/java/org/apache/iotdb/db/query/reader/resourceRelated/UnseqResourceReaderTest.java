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

package org.apache.iotdb.db.query.reader.resourceRelated;

import java.io.IOException;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.query.reader.IPointReaderByTimestamp;
import org.apache.iotdb.db.query.reader.ReaderTestHelper;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.Path;
import org.junit.Assert;
import org.junit.Test;

public class UnseqResourceReaderTest extends ReaderTestHelper {

  private QueryContext context = EnvironmentUtils.TEST_QUERY_CONTEXT;

  @Override
  protected void insertData() throws IOException, QueryProcessException {
    for (int j = 1; j <= 100; j++) {
      insertOneRecord(j, j);
    }
    storageGroupProcessor.getWorkSequenceTsFileProcessor().syncFlush();

    for (int j = 10; j >= 1; j--) {
      insertOneRecord(j, j);
    }
    for (int j = 11; j <= 20; j++) {
      insertOneRecord(j, j);
    }
    storageGroupProcessor.putAllWorkingTsFileProcessorIntoClosingList();

    for (int j = 21; j <= 30; j += 2) {
      insertOneRecord(j, 0); // will be covered when read
    }
    storageGroupProcessor.waitForAllCurrentTsFileProcessorsClosed();

    for (int j = 21; j <= 30; j += 2) {
      insertOneRecord(j, j);
    }
    storageGroupProcessor.waitForAllCurrentTsFileProcessorsClosed();

    insertOneRecord(2, 100);
  }

  @Test
  public void testUnseqResourceMergeReaderWithGlobalTimeFilter() throws IOException {
    Path path = new Path(deviceId, measurementId);
    QueryDataSource queryDataSource = storageGroupProcessor.query(deviceId, measurementId, context,
        null);
    IPointReader reader = null;
//    new UnseqResourceMergeReader(path,
//        queryDataSource.getUnseqResources(), EnvironmentUtils.TEST_QUERY_CONTEXT, TimeFilter.lt(4));

    int cnt = 0;
    while (reader.hasNext()) {
      cnt++;
      TimeValuePair timeValuePair = reader.next();
      Assert.assertEquals(cnt, timeValuePair.getTimestamp());
      if (cnt == 2) {
        Assert.assertEquals(100, timeValuePair.getValue().getInt());
      } else {
        Assert.assertEquals(cnt, timeValuePair.getValue().getInt());
      }
    }
    Assert.assertEquals(3, cnt);
  }

  @Test
  public void testUnseqResourceMergeReaderWithoutFilter() throws IOException {
    Path path = new Path(deviceId, measurementId);
    QueryDataSource queryDataSource = storageGroupProcessor
        .query(deviceId, measurementId, context, null);
    IPointReader reader = null;
//    new UnseqResourceMergeReader(path,
//        queryDataSource.getUnseqResources(), EnvironmentUtils.TEST_QUERY_CONTEXT, null);

    int cnt = 0;
    while (reader.hasNext()) {
      cnt++;
      TimeValuePair timeValuePair = reader.next();
      if (cnt == 2) {
        Assert.assertEquals(2, timeValuePair.getTimestamp());
        Assert.assertEquals(100, timeValuePair.getValue().getInt());
      } else if (cnt < 21) {
        Assert.assertEquals(cnt, timeValuePair.getTimestamp());
        Assert.assertEquals(cnt, timeValuePair.getValue().getInt());
      }
    }
    Assert.assertEquals(25, cnt);
  }

  @Test
  public void testUnseqResourceReaderByTimestamp() throws IOException {
    Path path = new Path(deviceId, measurementId);
    QueryDataSource queryDataSource = storageGroupProcessor.query(deviceId, measurementId, context,
        null);
    IPointReaderByTimestamp reader = new UnseqResourceReaderByTimestamp(path,
        queryDataSource.getUnseqResources(), EnvironmentUtils.TEST_QUERY_CONTEXT);

    for (long time = 0; time <= 25; time++) {
      Integer value = (Integer) reader.getValueInTimestamp(time);
      if (time == 0) {
        Assert.assertNull(value);
      } else if (time == 2) {
        Assert.assertEquals(100, (int) value);
      } else if (time >= 21 && time % 2 == 0) {
        Assert.assertNull(value);
      } else {
        Assert.assertEquals(time, (int) value);
      }
    }
    Assert.assertNull(reader.getValueInTimestamp(26));

    Assert.assertEquals(27, (int) reader.getValueInTimestamp(27));
    // Note that read by the same timestamp twice is an error demonstration.
    Assert.assertEquals(0, (int) reader.getValueInTimestamp(27));

    Assert.assertEquals(29, (int) reader.getValueInTimestamp(29));
    // Note that read by the same timestamp twice is an error demonstration.
    Assert.assertEquals(0, (int) reader.getValueInTimestamp(29));
  }
}
