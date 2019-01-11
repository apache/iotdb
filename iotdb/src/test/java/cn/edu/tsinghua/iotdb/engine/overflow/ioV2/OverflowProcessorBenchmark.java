package cn.edu.tsinghua.iotdb.engine.overflow.ioV2;

import cn.edu.tsinghua.iotdb.conf.IoTDBConfig;
import cn.edu.tsinghua.iotdb.conf.IoTDBDescriptor;
import cn.edu.tsinghua.iotdb.engine.bufferwrite.Action;
import cn.edu.tsinghua.iotdb.engine.bufferwrite.FileNodeConstants;
import cn.edu.tsinghua.iotdb.exception.OverflowProcessorException;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.tsinghua.tsfile.write.record.TSRecord;
import cn.edu.tsinghua.tsfile.write.record.datapoint.LongDataPoint;
import cn.edu.tsinghua.tsfile.write.schema.FileSchema;
import cn.edu.tsinghua.tsfile.write.schema.MeasurementSchema;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Overflow Insert Benchmark.
 * This class is used to bench overflow processor module and gets
 * its performance.
 */
public class OverflowProcessorBenchmark {

    private static final IoTDBConfig TsFileDBConf = IoTDBDescriptor.
            getInstance().getConfig();

    private static int numOfDevice = 100;
    private static int numOfMeasurement = 100;
    private static int numOfPoint = 1000;

    private static String[] deviceIds = new String[numOfDevice];
    static {
        for(int i = 0;i<numOfDevice;i++){
            deviceIds[i] = String.valueOf("d"+i);
        }
    }

    private static String[] measurementIds  = new String[numOfMeasurement];
    private static FileSchema fileSchema = new FileSchema();
    private static TSDataType tsDataType = TSDataType.INT64;

    static {
        for(int i = 0;i< numOfMeasurement;i++){
            measurementIds[i] = String.valueOf("m"+i);
            MeasurementSchema measurementDescriptor = new MeasurementSchema("m"+i,
                    tsDataType, TSEncoding.PLAIN);
            assert measurementDescriptor.getCompressor()!=null;
            fileSchema.registerMeasurement(measurementDescriptor);

        }
    }

    private static void before() throws IOException {
        FileUtils.deleteDirectory(new File(TsFileDBConf.overflowDataDir));
    }

    private static void after() throws IOException {
        FileUtils.deleteDirectory(new File(TsFileDBConf.overflowDataDir));
    }
    public static void main(String[] args) throws IOException, OverflowProcessorException {
        Map<String,Action> parameters = new HashMap<>();
        parameters.put(FileNodeConstants.OVERFLOW_FLUSH_ACTION, new Action() {
            @Override
            public void act() throws Exception {
                System.out.println(FileNodeConstants.OVERFLOW_FLUSH_ACTION);
            }
        });
        parameters.put(FileNodeConstants.FILENODE_PROCESSOR_FLUSH_ACTION, new Action() {
            @Override
            public void act() throws Exception {
                System.out.println(FileNodeConstants.FILENODE_PROCESSOR_FLUSH_ACTION);
            }
        });
        OverflowProcessor overflowProcessor = new OverflowProcessor(
                "Overflow_bench",parameters,fileSchema);
        long startTime = System.currentTimeMillis();
        for(int i = 0;i<numOfPoint;i++){
            for(int j = 0;j<numOfDevice;j++){
                TSRecord tsRecord = getRecord(deviceIds[j]);
                overflowProcessor.insert(tsRecord);
            }
        }
        long endTime = System.currentTimeMillis();
        overflowProcessor.close();
        System.out.println(String.format("Num of time series: %d, " +
                        "Num of points for each time series: %d, " +
                        "The total time: %d ms. ",numOfMeasurement*numOfDevice,
                numOfPoint,endTime-startTime));

        after();
    }

    private static TSRecord getRecord(String deviceId){
        long time = System.nanoTime();
        long value = System.nanoTime();
        TSRecord tsRecord = new TSRecord(time,deviceId);
        for(String measurement: measurementIds)
            tsRecord.addTuple(new LongDataPoint(measurement,value));
        return tsRecord;
    }
}
