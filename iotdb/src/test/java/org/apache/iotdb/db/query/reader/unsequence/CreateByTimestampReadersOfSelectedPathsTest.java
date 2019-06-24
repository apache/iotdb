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

import org.apache.iotdb.db.engine.MetadataManagerHelper;
import org.apache.iotdb.db.engine.filenodeV2.FileNodeManagerV2;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.query.factory.SeriesReaderFactoryImpl;
import org.apache.iotdb.db.query.reader.merge.EngineReaderByTimeStamp;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CreateByTimestampReadersOfSelectedPathsTest {
    private String systemDir = "data/info";
    private String deviceId = "root.vehicle.d0";
    private String measurementId = "s0";

    @Before
    public void setUp() throws Exception {
        MetadataManagerHelper.initMetadata();
        EnvironmentUtils.envSetUp();
    }

    @After
    public void tearDown() throws Exception {
        EnvironmentUtils.cleanEnv();
        EnvironmentUtils.cleanDir(systemDir);
    }

    @Test
    public void testUnSeqReaderWithoutFilter() throws IOException, FileNodeManagerException {
        // write
        for (int j = 1; j <= 10; j++) {
            TSRecord record = new TSRecord(j, deviceId);
            record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
            FileNodeManagerV2.getInstance().insert(new InsertPlan(record));
            FileNodeManagerV2.getInstance().asyncFlushAndSealAllFiles();
        }

        for (int j = 10; j >= 1; j--) {
            TSRecord record = new TSRecord(j, deviceId);
            record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
            FileNodeManagerV2.getInstance().insert(new InsertPlan(record));
            FileNodeManagerV2.getInstance().asyncFlushAndSealAllFiles();
        }
        TSRecord record = new TSRecord(2, deviceId);
        record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(100)));
        FileNodeManagerV2.getInstance().insert(new InsertPlan(record));
        FileNodeManagerV2.getInstance().asyncFlushAndSealAllFiles();

        // query
        List<Path> paths = new ArrayList<>();
        paths.add(new Path(deviceId, measurementId));
        List<EngineReaderByTimeStamp> readers = SeriesReaderFactoryImpl.getInstance().
                createByTimestampReadersOfSelectedPaths(paths, EnvironmentUtils.TEST_QUERY_CONTEXT);
        Assert.assertEquals(1, readers.size());
        EngineReaderByTimeStamp reader = readers.get(0);

        for (long time = 1; time <= 10; time++) {
            // NOTE that the timestamps should be in be in strictly increasing order.
            int value = (Integer) reader.getValueInTimestamp(time);
            if (time == 2) {
                Assert.assertEquals(100, value);
            } else {
                Assert.assertEquals(time, value);
            }
        }
    }
}
