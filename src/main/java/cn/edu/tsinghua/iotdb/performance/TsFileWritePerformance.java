package cn.edu.tsinghua.iotdb.performance;

import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.tsinghua.tsfile.timeseries.write.TsFileWriter;
import cn.edu.tsinghua.tsfile.timeseries.write.desc.MeasurementDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.WriteProcessException;
import cn.edu.tsinghua.tsfile.timeseries.write.record.DataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;
import cn.edu.tsinghua.tsfile.timeseries.write.schema.FileSchema;

import java.io.File;
import java.io.IOException;

public class TsFileWritePerformance {

    private static final String outPutFilePath = "src/main/resources/output";

    public static void main(String[] args) throws WriteProcessException, IOException {
        writeMultiSeriesTest();
        writeSingleSeriesTest();
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

    private static void writeSingleSeriesTest() throws WriteProcessException, IOException {
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
}
