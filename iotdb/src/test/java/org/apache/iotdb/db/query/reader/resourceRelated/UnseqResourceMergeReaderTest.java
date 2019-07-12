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

package org.apache.iotdb.db.query.reader.resourceRelated;

import java.io.IOException;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.query.reader.IReaderByTimestamp;
import org.apache.iotdb.db.query.reader.ReaderTestHelper;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.junit.Assert;
import org.junit.Test;

public class UnseqResourceMergeReaderTest extends ReaderTestHelper {

    private QueryContext context = EnvironmentUtils.TEST_QUERY_CONTEXT;

    @Override
    protected void insertData() {
        for (int j = 1; j <= 10; j++) {
            insertOneRecord(j, j);
        }
        storageGroupProcessor.getWorkSequenceTsFileProcessor().asyncFlush();

        for (int j = 10; j >= 1; j--) {
            insertOneRecord(j, j);
        }
        storageGroupProcessor.getWorkSequenceTsFileProcessor().asyncFlush();

        insertOneRecord(2, 100);
    }

    @Test
    public void testUnseqSeriesReaderWithGlobalTimeFilter() throws IOException {
        Path path = new Path(deviceId, measurementId);
        QueryDataSource queryDataSource = storageGroupProcessor
            .query(deviceId, measurementId, context);
        IPointReader reader = new UnseqResourceMergeReader(path,
            queryDataSource.getUnseqResources(), EnvironmentUtils.TEST_QUERY_CONTEXT,
            TimeFilter.eq(4));
        int cnt = 0;
        while (reader.hasNext()) {
            cnt++;
            TimeValuePair timeValuePair = reader.next();
            Assert.assertEquals(4, timeValuePair.getTimestamp());
            Assert.assertEquals(4, timeValuePair.getValue().getInt());
        }
        Assert.assertEquals(1, cnt);
    }

    @Test
    public void testUnseqSeriesReaderWithoutFilter() throws IOException {
        Path path = new Path(deviceId, measurementId);
        QueryDataSource queryDataSource = storageGroupProcessor
            .query(deviceId, measurementId, context);
        IPointReader reader = new UnseqResourceMergeReader(path,
            queryDataSource.getUnseqResources(), EnvironmentUtils.TEST_QUERY_CONTEXT, null);
        int cnt = 0;
        while (reader.hasNext()) {
            cnt++;
            TimeValuePair timeValuePair = reader.next();
            if (cnt == 2) {
                Assert.assertEquals(2, timeValuePair.getTimestamp());
                Assert.assertEquals(100, timeValuePair.getValue().getInt());
            } else {
                Assert.assertEquals(cnt, timeValuePair.getTimestamp());
                Assert.assertEquals(cnt, timeValuePair.getValue().getInt());
            }
        }
        Assert.assertEquals(10, cnt);
    }

    @Test
    public void testUnseqSeriesReaderByTimestamp() throws IOException, StorageEngineException {
        // write
        for (int j = 1; j <= 10; j++) {
            TSRecord record = new TSRecord(j, deviceId);
            record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
            StorageEngine.getInstance().insert(new InsertPlan(record));
            StorageEngine.getInstance().asyncFlushAndSealAllFiles();
        }

//        for (int j = 10; j >= 1; j--) {
//            TSRecord record = new TSRecord(j, deviceId);
//            record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
//            StorageEngine.getInstance().insert(new InsertPlan(record));
//            StorageEngine.getInstance().asyncFlushAndSealAllFiles();
//        }
        TSRecord record = new TSRecord(2, deviceId);
        record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(100)));
        StorageEngine.getInstance().insert(new InsertPlan(record));
        //       StorageEngine.getInstance().asyncFlushAndSealAllFiles();

        // query
        Path path = new Path(deviceId, measurementId);
        QueryDataSource queryDataSource = storageGroupProcessor
            .query(deviceId, measurementId, context);
        IReaderByTimestamp reader = new UnseqResourceReaderByTimestamp(path,
            queryDataSource.getUnseqResources(), EnvironmentUtils.TEST_QUERY_CONTEXT);

        for (long time = 1; time <= 10; time++) {
            // NOTE that the timestamps should be in be in strictly increasing order.
            Integer value = (Integer) reader.getValueInTimestamp(time);
            if (time == 2) {
                Assert.assertEquals(100, (int) value);
            } else {
                Assert.assertEquals(time, (int) value);
            }
        }
    }
}
