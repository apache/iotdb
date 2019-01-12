package org.apache.iotdb.db.engine.bufferwrite;

import org.apache.iotdb.db.exception.BufferWriteProcessorException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.LongDataPoint;
import org.apache.iotdb.tsfile.write.schema.FileSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.commons.io.FileUtils;
import org.apache.iotdb.db.exception.BufferWriteProcessorException;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * BufferWrite insert Benchmark.
 * This class is used to bench Bufferwrite module and gets its performance.
 */
public class BufferWriteBenchmark {
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
        FileUtils.deleteDirectory(new File("BufferBenchmark"));
    }

    private static void after() throws IOException {
        FileUtils.deleteDirectory(new File("BufferBenchmark"));
    }

    public static void main(String[] args) throws BufferWriteProcessorException, IOException {
        before();
        Map<String,Action> parameters = new HashMap<>();
        parameters.put(FileNodeConstants.BUFFERWRITE_FLUSH_ACTION, new Action() {
            @Override
            public void act() throws Exception {
                System.out.println(FileNodeConstants.BUFFERWRITE_FLUSH_ACTION);
            }
        });
        parameters.put(FileNodeConstants.BUFFERWRITE_CLOSE_ACTION, new Action() {
            @Override
            public void act() throws Exception {
                System.out.println(FileNodeConstants.BUFFERWRITE_CLOSE_ACTION);
            }
        });
        parameters.put(FileNodeConstants.FILENODE_PROCESSOR_FLUSH_ACTION, new Action() {
            @Override
            public void act() throws Exception {
                System.out.println(FileNodeConstants.FILENODE_PROCESSOR_FLUSH_ACTION);
            }
        });

        BufferWriteProcessor bufferWriteProcessor = new BufferWriteProcessor(
                "BufferBenchmark","bench","benchFile",parameters,fileSchema);

        long startTime = System.currentTimeMillis();
        for(int i = 0;i<numOfPoint;i++){
            for(int j = 0;j<numOfDevice;j++){
                TSRecord tsRecord = getRecord(deviceIds[j]);
                bufferWriteProcessor.write(tsRecord);
            }
        }
        long endTime = System.currentTimeMillis();
        bufferWriteProcessor.close();
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
