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
import org.apache.iotdb.db.engine.storagegroup.TsFileProcessor;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.reader.ReaderTestHelper;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;
import org.junit.Assert;
import org.junit.Test;

public class NewUnseqResourceMergeReaderTest extends ReaderTestHelper{

  private QueryContext context = EnvironmentUtils.TEST_QUERY_CONTEXT;

  /**
   * chunk : [1,10], [4,4], [10,10], [20,20]
   */
  @Override protected void insertData() throws IOException, QueryProcessException {
    insertOneRecord(1000, 1000);
    storageGroupProcessor.putAllWorkingTsFileProcessorIntoClosingList();

    insertOneRecord(1, 1);
    insertOneRecord(10, 10);
    for (TsFileProcessor tsfileProcessor : storageGroupProcessor.getWorkUnsequenceTsFileProcessor()) {
      tsfileProcessor.syncFlush();
    }

    insertOneRecord(4, 100);
    for (TsFileProcessor tsfileProcessor : storageGroupProcessor.getWorkUnsequenceTsFileProcessor()) {
      tsfileProcessor.syncFlush();
    }

    insertOneRecord(10, 1000);
    for (TsFileProcessor tsfileProcessor : storageGroupProcessor.getWorkUnsequenceTsFileProcessor()) {
      tsfileProcessor.syncFlush();
    }

    insertOneRecord(20, 20);
    storageGroupProcessor.putAllWorkingTsFileProcessorIntoClosingList();
  }

  @Test
  public void testNewUnseqResourceMergeReaderWithoutFilter() throws IOException {
    Path path = new Path(deviceId, measurementId);
    QueryDataSource queryDataSource = storageGroupProcessor
        .query(deviceId, measurementId, context, null);
    IBatchReader reader = new NewUnseqResourceMergeReader(path, dataType,
        queryDataSource.getUnseqResources(), EnvironmentUtils.TEST_QUERY_CONTEXT, null);

    int cnt = 0;
    BatchData batchData;
    while (reader.hasNextBatch()) {
      batchData = reader.nextBatch();
      while (batchData.hasCurrent()) {
        cnt++;
        batchData.next();
      }
    }
    Assert.assertEquals(4, cnt);
  }


  @Test
  public void testNewUnseqResourceMergeReaderWithTimeFilter() throws IOException {
    Path path = new Path(deviceId, measurementId);
    QueryDataSource queryDataSource = storageGroupProcessor
        .query(deviceId, measurementId, context, null);
    IBatchReader reader = new NewUnseqResourceMergeReader(path, dataType,
        queryDataSource.getUnseqResources(), EnvironmentUtils.TEST_QUERY_CONTEXT, FilterFactory.and(
        TimeFilter.gtEq(3L), TimeFilter.ltEq(5L)));

    int cnt = 0;
    BatchData batchData;
    while (reader.hasNextBatch()) {
      batchData = reader.nextBatch();
      while (batchData.hasCurrent()) {
        cnt++;
        batchData.next();
      }
    }
    Assert.assertEquals(1, cnt);
  }

}
