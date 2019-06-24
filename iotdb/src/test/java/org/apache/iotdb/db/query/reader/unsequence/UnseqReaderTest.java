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

package org.apache.iotdb.db.query.reader.unsequence;

import org.apache.iotdb.db.engine.querycontext.QueryDataSourceV2;
import org.apache.iotdb.db.query.factory.SeriesReaderFactoryImpl;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.query.reader.ReaderTestHelper;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class UnseqReaderTest extends ReaderTestHelper {
    @Override
    protected void insertData() {
        for (int j = 1; j <= 10; j++) {
            insertOneRecord(j, j);
        }
        fileNodeProcessorV2.getWorkSequenceTsFileProcessor().asyncFlush();

        for (int j = 10; j >= 1; j--) {
            insertOneRecord(j, j);
        }
        fileNodeProcessorV2.getWorkSequenceTsFileProcessor().asyncFlush();

        insertOneRecord(2, 100);
    }

    @Test
    public void testUnSeqReaderWithGlobalTimeFilter() throws IOException {
        Path path = new Path(deviceId, measurementId);
        QueryDataSourceV2 queryDataSource = fileNodeProcessorV2.query(deviceId, measurementId);
        IPointReader reader = SeriesReaderFactoryImpl.getInstance().createUnSeqReader(path,
                queryDataSource.getUnseqResources(), EnvironmentUtils.TEST_QUERY_CONTEXT, TimeFilter.eq(4));
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
    public void testUnSeqReaderWithoutFilter() throws IOException {
        Path path = new Path(deviceId, measurementId);
        QueryDataSourceV2 queryDataSource = fileNodeProcessorV2.query(deviceId, measurementId);
        IPointReader reader = SeriesReaderFactoryImpl.getInstance().createUnSeqReader(path,
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

    // Note that createUnSeqByTimestampReader is of private use.
}
