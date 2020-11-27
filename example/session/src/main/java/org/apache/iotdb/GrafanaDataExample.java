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
package org.apache.iotdb;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class GrafanaDataExample {
    private static Session session;
    public static void main(String[] args) throws IoTDBConnectionException, StatementExecutionException {
        System.out.println(System.currentTimeMillis());
        session = new Session("127.0.0.1", 6667, "root", "root");
        session.open(false);
        insertTablet();
    }


    /**
     * insert the data of a device. For each timestamp, the number of measurements is the same.
     *
     * a Tablet example:
     *
     *      device1
     * time s1, s2, s3
     * 1,   1,  1,  1
     * 2,   2,  2,  2
     * 3,   3,  3,  3
     *
     * Users need to control the count of Tablet and write a batch when it reaches the maxBatchSize
     */
    private static void insertTablet() throws IoTDBConnectionException, StatementExecutionException {
        // The schema of measurements of one device
        // only measurementId and data type in MeasurementSchema take effects in Tablet
        List<MeasurementSchema> schemaList = new ArrayList<>();
        schemaList.add(new MeasurementSchema("s1", TSDataType.INT64));
        schemaList.add(new MeasurementSchema("s2", TSDataType.INT64));
        schemaList.add(new MeasurementSchema("s3", TSDataType.INT64));

        Tablet tablet = new Tablet("root.sg1.d1", schemaList, 100000);

        //Method 1 to add tablet data
        long timestamp = System.currentTimeMillis();
        for (int row = 0; row < 1000; row++) {
            tablet.addTimestamp(row, timestamp - row * 1000);
            for (int s = 0; s < 3; s++) {
                tablet.addValue(schemaList.get(s).getMeasurementId(), row, (long) row);
            }
            tablet.rowSize++;
        }

        session.insertTablet(tablet);
        tablet.reset();
    }

    /**
     * insert the data of a device. For each timestamp, the number of measurements is the same.
     *
     * a Tablet example:
     *
     *      device1
     * time s1, s2, s3
     * 1,   1,  1,  1
     * 2,   2,  2,  2
     * 3,   3,  3,  3
     *
     * Users need to control the count of Tablet and write a batch when it reaches the maxBatchSize
     */
    private static void insertTabletTest() throws IoTDBConnectionException, StatementExecutionException {
        // The schema of measurements of one device
        // only measurementId and data type in MeasurementSchema take effects in Tablet
        List<MeasurementSchema> schemaList = new ArrayList<>();
        schemaList.add(new MeasurementSchema("s1", TSDataType.INT64));
        schemaList.add(new MeasurementSchema("s2", TSDataType.INT64));
        schemaList.add(new MeasurementSchema("s3", TSDataType.INT64));

        Tablet tablet = new Tablet("root.sg1.d1", schemaList, 100);


        for (long row = 0; row < 10000; row++) {
            int rowIndex = tablet.rowSize++;
            tablet.addTimestamp(rowIndex, row);
            for (int s = 0; s < 3; s++) {
                long value = new Random().nextLong();
                tablet.addValue(schemaList.get(s).getMeasurementId(), rowIndex, value);
            }
            if (tablet.rowSize == tablet.getMaxRowNumber()) {
                session.insertTablet(tablet, true);
                tablet.reset();
            }
        }

        if (tablet.rowSize != 0) {
            session.insertTablet(tablet);
            tablet.reset();
        }

        //Method 2 to add tablet data
        long[] timestamps = tablet.timestamps;
        Object[] values = tablet.values;

        for (long time = 0; time < 100; time++) {
            int row = tablet.rowSize++;
            timestamps[row] = time;
            for (int i = 0; i < 3; i++) {
                long[] sensor = (long[]) values[i];
                sensor[row] = i;
            }
            if (tablet.rowSize == tablet.getMaxRowNumber()) {
                session.insertTablet(tablet, true);
                tablet.reset();
            }
        }

        if (tablet.rowSize != 0) {
            session.insertTablet(tablet);
            tablet.reset();
        }
    }
}
