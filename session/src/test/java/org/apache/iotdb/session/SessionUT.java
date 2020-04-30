package org.apache.iotdb.session;

import org.apache.iotdb.rpc.BatchExecutionException;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SessionUT {

    public static void main(String[] args) throws BatchExecutionException, IoTDBConnectionException {
        /*
        To test sortTablet in Class Session
        !!!
        Before testing, change the sortTablet from private method to public method
        !!!
         */
        Session session = new Session("127.0.0.1", 6667, "root", "root");
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
        System.out.printf("%s\t%s\n", "timestamp", "s1");
        for (int i = 0; i < 3; i++) {
            System.out.println(timestamps[i] + "\t\t\t" + sensor[i]);
        }

        session.sortTablet(tablet);

        /*
        After sorting, if the tablet data is sorted according to the timestamps,
        data in tablet will be
        timestamp   s1
        0           1
        1           0
        2           2

        If the data equal to above tablet, test pass, otherwise test fialed
         */
        long[] resTimestamps = tablet.timestamps;
        long[] resValues = (long[])tablet.values[0];
        boolean ifPass = false;
        if (resTimestamps[0] == 0 && resTimestamps[1] == 1 && resTimestamps[2] == 2) {
            if (resValues[0] == 1 && resValues[1] == 2 && resValues[2] == 0) {
                ifPass = true;
            }
        }

        System.out.printf("Test Result: %s\n", ifPass? "Yes" : "NO");
        System.out.printf("%s\t%s\n", "timestamp", "s1");
        for (int i = 0; i < 3; i++) {
            System.out.println(resTimestamps[i] + "\t\t\t" + resValues[i]);
        }

    }
}
