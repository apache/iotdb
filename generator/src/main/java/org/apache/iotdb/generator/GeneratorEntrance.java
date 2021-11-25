package org.apache.iotdb.generator;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.ClusterSession;
import org.apache.iotdb.tsfile.write.record.Tablet;

public class GeneratorEntrance {
    public static void main(String[] args) throws IoTDBConnectionException, StatementExecutionException, InterruptedException {
        String []addressElements = args[1].split(":");
        String seriesPath = args[2];
        int timeInterval = Integer.parseInt(args[3])*1000;
        int batchNum = Integer.parseInt(args[4]);
        String []pathElements = seriesPath.split(".");
        String measurementId = pathElements[pathElements.length-1];
        String deviceId = seriesPath.substring(0,seriesPath.length()-measurementId.length()-1);

        ClusterSession clusterSession = new ClusterSession(addressElements[0],Integer.parseInt(addressElements[1]));
        clusterSession.open();

        long timestampForInsert = 0;
        while (true){
            long startTime =  System.currentTimeMillis();
            Tablet tablet = Generator.generateTablet(deviceId,pathElements[pathElements.length-1],timestampForInsert,batchNum);
            clusterSession.insertTablet(tablet);
            timestampForInsert += batchNum;
            long endTime = System.currentTimeMillis();
            Thread.sleep(timeInterval-(endTime-startTime));
        }
    }
}
