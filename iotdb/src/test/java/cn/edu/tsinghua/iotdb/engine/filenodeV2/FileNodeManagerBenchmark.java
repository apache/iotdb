package cn.edu.tsinghua.iotdb.engine.filenodeV2;

import cn.edu.tsinghua.iotdb.engine.filenode.FileNodeManager;
import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;
import cn.edu.tsinghua.iotdb.exception.MetadataArgsErrorException;
import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.metadata.MManager;
import cn.edu.tsinghua.iotdb.postback.utils.RandomNum;
import cn.edu.tsinghua.iotdb.utils.EnvironmentUtils;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;
import cn.edu.tsinghua.tsfile.timeseries.write.record.datapoint.LongDataPoint;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Bench The filenode manager with mul-thread and get its performance.
 */
public class FileNodeManagerBenchmark {

    private static int numOfWoker = 10;
    private static int numOfDevice = 10;
    private static int numOfMeasurement = 10;
    private static long numOfTotalLine = 10000000;
    private static CountDownLatch latch = new CountDownLatch(numOfWoker);
    private static AtomicLong atomicLong = new AtomicLong();

    private static String[] devices = new String[numOfDevice];
    private static String prefix = "root.bench";
    static {
        for(int i = 0;i<numOfDevice;i++){
            devices[i] = prefix+"."+"device_"+i;
        }
    }

    private static String[] measurements = new String[numOfMeasurement];
    static {
        for(int i = 0;i<numOfMeasurement;i++){
            measurements[i] = "measurement_"+i;
        }
    }

    private static void prepare() throws MetadataArgsErrorException, PathErrorException, IOException {
        MManager manager = MManager.getInstance();
        manager.setStorageLevelToMTree(prefix);
        for(String device:devices){
            for(String measurement:measurements){
                manager.addPathToMTree(device+"."+measurement, TSDataType.INT64.toString(), TSEncoding.PLAIN.toString(),new String[0]);
            }
        }
    }

    private static void tearDown() throws IOException {
        EnvironmentUtils.cleanEnv();
    }

    public static void main(String[] args) throws InterruptedException, IOException, MetadataArgsErrorException, PathErrorException {
        tearDown();
        prepare();
        long startTime = System.currentTimeMillis();
        for(int i = 0;i<numOfWoker;i++){
            Woker woker = new Woker();
            woker.start();
        }
        latch.await();
        long endTime = System.currentTimeMillis();
        tearDown();
        System.out.println(String.format("The total time: %d ms",(endTime-startTime)));
    }

    private static class Woker extends Thread{
        @Override
        public void run() {
            try{
                while(true){
                    long seed = atomicLong.addAndGet(1);
                    if(seed>numOfTotalLine){
                        break;
                    }
                    long time = RandomNum.getRandomLong(1,seed);
                    String deltaObject = devices[(int) (time%numOfDevice)];
                    TSRecord tsRecord = getRecord(deltaObject,time);
                    FileNodeManager.getInstance().insert(tsRecord,true);
                }
            } catch (FileNodeManagerException e) {
                e.printStackTrace();
            } finally {
                latch.countDown();
            }
        }
    }

    private static TSRecord getRecord(String deltaObjectId, long timestamp){
        TSRecord tsRecord = new TSRecord(timestamp,deltaObjectId);
        for(String measurement:measurements){
            tsRecord.addTuple(new LongDataPoint(measurement,timestamp));
        }
        return tsRecord;
    }
}
