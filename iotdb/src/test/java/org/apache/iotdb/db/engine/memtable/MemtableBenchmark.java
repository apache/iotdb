package org.apache.iotdb.db.engine.memtable;


import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

/**
 * Memtable insert benchmark.
 * Bench the Memtable and get its performance.
 */
public class MemtableBenchmark {

    private static String deviceId = "d0";
    private static int numOfMeasurement = 10000;
    private static int numOfPoint = 1000;

    private static String[] measurementId  = new String[numOfMeasurement];

    static {
        for(int i = 0;i< numOfMeasurement;i++){
            measurementId[i] = "m"+i;
        }
    }

    private static TSDataType tsDataType = TSDataType.INT64;


    public static void main(String[] args) {
        IMemTable memTable = new PrimitiveMemTable();
        final long startTime = System.currentTimeMillis();
        // cpu not locality
        for(int i = 0;i<numOfPoint;i++){
            for(int j = 0;j<numOfMeasurement;j++){
                memTable.write(deviceId,measurementId[j],tsDataType,System.nanoTime(),String.valueOf(System.currentTimeMillis()));
            }
        }

        final long endTime = System.currentTimeMillis();
        System.out.println(String.format("Num of time series: %d, " +
                "Num of points for each time series: %d, " +
                "The total time: %d ms. ",numOfMeasurement,numOfPoint,endTime-startTime));
    }
}
