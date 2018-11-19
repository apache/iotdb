package cn.edu.tsinghua.iotdb.performance;

import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TsPrimitiveType;
import cn.edu.tsinghua.tsfile.timeseries.write.TsFileWriter;
import cn.edu.tsinghua.tsfile.timeseries.write.desc.MeasurementDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.WriteProcessException;
import cn.edu.tsinghua.tsfile.timeseries.write.record.DataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;
import cn.edu.tsinghua.tsfile.timeseries.write.schema.FileSchema;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RecordConstructionPerformance {

    private static final String outPutFilePath = "src/main/resources/output";

    public static void main(String[] args) throws WriteProcessException, IOException {
        //writeMultiSeriesTest();
        //writeSingleSeriesTestWithMultiRecordConstruction();
        writeSingleSeriesTestWithOneRecordConstruction();
    }

    private static void writeMultiSeriesTest() throws WriteProcessException, IOException {
        long startTime = System.currentTimeMillis();

        FileSchema fileSchema = new FileSchema();
        File outPutFile = new File(outPutFilePath);
        for (int i = 1; i <= 100; i++)
            fileSchema.registerMeasurement(new MeasurementDescriptor("s" + i, TSDataType.FLOAT, TSEncoding.RLE));
        TsFileWriter fileWriter = new TsFileWriter(outPutFile, fileSchema, TSFileDescriptor.getInstance().getConfig());

        for (int i = 1; i <= 500; i++) {
            for (int j = 1; j <= 100; j++) {
                for (long time = 1; time <= 2257; time++) {
                    TSRecord record = new TSRecord(time, "d" + i);
                    record.addTuple(DataPoint.getDataPoint(TSDataType.FLOAT, "s" + j, String.valueOf(4.0)));
                    fileWriter.write(record);
                }
            }
        }

        long endTime = System.currentTimeMillis();
        System.out.println(String.format("write multi series time cost %dms", endTime - startTime));
    }

    private static void writeSingleSeriesTestWithMultiRecordConstruction() throws WriteProcessException, IOException {
        long startTime = System.currentTimeMillis();

        FileSchema fileSchema = new FileSchema();
        File outPutFile = new File(outPutFilePath);
        fileSchema.registerMeasurement(new MeasurementDescriptor("s0", TSDataType.FLOAT, TSEncoding.RLE));
        TsFileWriter fileWriter = new TsFileWriter(outPutFile, fileSchema, TSFileDescriptor.getInstance().getConfig());

        for (int i = 1; i <= 112850000; i++) {
            TSRecord record = new TSRecord(i, "d0");
            record.addTuple(DataPoint.getDataPoint(TSDataType.FLOAT, "s0" , String.valueOf(4.0)));
            fileWriter.write(record);
        }

        long endTime = System.currentTimeMillis();
        System.out.println(String.format("write single series time cost %dms", endTime - startTime));
    }

    private static final int size = 50000000;
    private static void writeSingleSeriesTestWithOneRecordConstruction() {

        List<TimeValuePair> values = new ArrayList<>();
        for (long i = 1; i <= size; i++) {
            TimeValuePair tp = new TimeValuePair(i, new TsPrimitiveType.TsFloat(i));
            values.add(tp);
        }

        long startTime = System.currentTimeMillis();
        for (int i = 0; i < size; i++) {
            TSRecord record = constructTsRecord(values.get(i), "d1", "s1");
            //fileWriter.write(record);
        }

        long endTime = System.currentTimeMillis();
        System.out.println(String.format("write single series time cost %dms", endTime - startTime));
    }

    private static TSRecord constructTsRecord(TimeValuePair timeValuePair, String deltaObjectId, String measurementId) {
        TSRecord record = new TSRecord(timeValuePair.getTimestamp(), deltaObjectId);
        record.addTuple(DataPoint.getDataPoint(timeValuePair.getValue().getDataType(), measurementId,
                timeValuePair.getValue().getValue().toString()));
        return record;
    }
}
