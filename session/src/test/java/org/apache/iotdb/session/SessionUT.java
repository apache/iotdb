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
package org.apache.iotdb.session;

import java.util.ArrayList;
import java.util.List;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class SessionUT {

    private Session session;

    @Before
    public void setUp() {
        System.setProperty(IoTDBConstant.IOTDB_CONF, "src/test/resources/");
        EnvironmentUtils.closeStatMonitor();
        EnvironmentUtils.envSetUp();
    }

    @After
    public void tearDown() throws Exception {
        session.close();
        EnvironmentUtils.cleanEnv();
    }

    @Test
    public void testSortTablet() {
        /*
        To test sortTablet in Class Session
        !!!
        Before testing, change the sortTablet from private method to public method
        !!!
         */
        session = new Session("127.0.0.1", 6667, "root", "root");
        List<MeasurementSchema> schemaList = new ArrayList<>();
        schemaList.add(new MeasurementSchema("s1",TSDataType.INT64, TSEncoding.RLE));
        // insert three rows data
        Tablet tablet = new Tablet("root.sg1.d1", schemaList, 3);
        long[] timestamps = tablet.timestamps;
        Object[] values = tablet.values;

        /*
        inorder data before inserting
        timestamp   s1
        2           0
        0           1
        1           2
         */
        // inorder timestamps
        timestamps[0] = 2;
        timestamps[1] = 0;
        timestamps[2] = 1;
        // just one column INT64 data
        long[] sensor = (long[]) values[0];
        sensor[0] = 0;
        sensor[1] = 1;
        sensor[2] = 2;
        tablet.rowSize = 3;

        session.sortTablet(tablet);
        
        /*
        After sorting, if the tablet data is sorted according to the timestamps,
        data in tablet will be
        timestamp   s1
        0           1
        1           2
        2           0

        If the data equal to above tablet, test pass, otherwise test fialed
         */
        long[] resTimestamps = tablet.timestamps;
        long[] resValues = (long[])tablet.values[0];
        long[] expectedTimestamps = new long[]{0, 1, 2};
        long[] expectedValues = new long[]{1,2,0};
        try {
            assertArrayEquals(expectedTimestamps, resTimestamps);
            assertArrayEquals(expectedValues, resValues);
        }
        catch (Exception e) {
            fail();
        }
    }

    @Test
    public void testInsertByStrAndSelectFailedData() throws IoTDBConnectionException, StatementExecutionException {
        session = new Session("127.0.0.1", 6667, "root", "root");
        session.open();

        String deviceId = "root.sg1.d1";

        session.createTimeseries(deviceId + "s1", TSDataType.INT64, TSEncoding.RLE, CompressionType.UNCOMPRESSED);
        session.createTimeseries(deviceId + "s2", TSDataType.INT64, TSEncoding.RLE, CompressionType.UNCOMPRESSED);
        session.createTimeseries(deviceId + "s3", TSDataType.INT64, TSEncoding.RLE, CompressionType.UNCOMPRESSED);

        List<MeasurementSchema> schemaList = new ArrayList<>();
        schemaList.add(new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.RLE));
        schemaList.add(new MeasurementSchema("s2", TSDataType.DOUBLE, TSEncoding.RLE));
        schemaList.add(new MeasurementSchema("s3", TSDataType.TEXT, TSEncoding.PLAIN));

        Tablet tablet = new Tablet("root.sg1.d1", schemaList, 10);

        long[] timestamps = tablet.timestamps;
        Object[] values = tablet.values;

        for (long time = 0; time < 10; time++) {
            int row = tablet.rowSize++;
            timestamps[row] = time;
            long[] sensor = (long[]) values[0];
            sensor[row] = time;
            double[] sensor2 = (double[]) values[1];
            sensor2[row] = 0.1 + time;
            Binary[] sensor3 = (Binary[]) values[2];
            sensor3[row] = Binary.valueOf("ha" + time);
        }

        if (tablet.rowSize != 0) {
            session.insertTablet(tablet);
            tablet.reset();
        }

        SessionDataSet dataSet = session.executeQueryStatement("select * from root.sg1.d1");
        int i = 0;
        while (dataSet.hasNext()) {
            RowRecord record = dataSet.next();
            System.out.println(record.toString());
            assertEquals(i, record.getFields().get(1).getLongV());
            assertTrue(record.getFields().get(2).isNull());
            assertTrue(record.getFields().get(3).isNull());
            i++;
        }
    }

}
