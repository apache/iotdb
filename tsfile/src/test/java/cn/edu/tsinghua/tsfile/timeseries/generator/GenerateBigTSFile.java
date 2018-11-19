package cn.edu.tsinghua.tsfile.timeseries.generator;


import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;
import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.common.constant.JsonFormatConstant;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.tsinghua.tsfile.timeseries.utils.FileUtils;
import cn.edu.tsinghua.tsfile.timeseries.utils.RecordUtils;
import cn.edu.tsinghua.tsfile.timeseries.utils.StringContainer;
import cn.edu.tsinghua.tsfile.timeseries.write.TsFileWriter;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.WriteProcessException;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;
import cn.edu.tsinghua.tsfile.timeseries.write.schema.FileSchema;

public class GenerateBigTSFile {
    private static final Logger LOG = LoggerFactory.getLogger(GenerateBigTSFile.class);
    private static TsFileWriter writer;
    private static String outputDataFile;
    private static TSFileConfig conf = TSFileDescriptor.getInstance().getConfig();

    private static int setRowGroupSize = conf.groupSizeInByte;
    // To be configure
    private static int deviceCount = 3;
    // s0:broken line
    // s1:line
    // s2:sin
    // s3:square wave
    // s4:log
    private static int[][] brokenLineConfigs = {{100, 1, 100}, {0, -1, 200000}, {10000, 2, 50000}};
    private static long[][] lineConfigs = {{1L << 32, 1}, {0, -1}, {10000, 2}};
    private static float[] squareAmplitude = {12.5f, 1273.143f, 1823767.4f};
    private static float[] squareBaseLine = {25f, 1273.143f, 1823767.4f};
    private static int[] squareLength = {150, 5000, 20000};
    // y = A*sin(wt), sinConfigs:w,A
    private static double[][] sinConfigs = {{0.05, 10}, {0.3, 100}, {2, 50}};
    private static double[][] sinAbnormalConfigs = {{0.8, 20}, {0.3, 100}, {2, 50}};
    private static String deltaObjectType = "root.laptop";


    private static void getNextRecord(long timestamp, long index) throws IOException {
        for (int i = 0; i < deviceCount; i++) {
            StringContainer sc = new StringContainer(",");
            sc.addTail("d" + i, timestamp, deltaObjectType);
            if (sensorSet.contains("s0")) {
                // s0:broken line, int
                if ((timestamp % brokenLineConfigs[i][2]) == 0)
                    brokenLineConfigs[i][1] = -brokenLineConfigs[i][1];
                brokenLineConfigs[i][0] += brokenLineConfigs[i][1];
                sc.addTail("s0", brokenLineConfigs[i][0]);
            }
            if (sensorSet.contains("s1")) {
                // s1:line, long
                lineConfigs[i][0] += lineConfigs[i][1];
                if (lineConfigs[i][0] < 0)
                    lineConfigs[i][0] = 0;
                sc.addTail("s1", lineConfigs[i][0]);
            }
            if (sensorSet.contains("s2")) {
                // s2:square wave, float
                if ((timestamp % squareLength[i]) == 0)
                    squareAmplitude[i] = -squareAmplitude[i];
                sc.addTail("s2", squareBaseLine[i] + squareAmplitude[i]);
            }
            if (sensorSet.contains("s3")) {
                // s3:sin, double
                if (index > 5000 && index < 8000)
                    sc.addTail(
                            "s3",
                            sinAbnormalConfigs[i][1] + sinAbnormalConfigs[i][1]
                                    * Math.sin(sinAbnormalConfigs[i][0] * timestamp));
                else
                    sc.addTail(
                            "s3",
                            sinConfigs[i][1] + sinConfigs[i][1]
                                    * Math.sin(sinConfigs[i][0] * timestamp));
            }
            strLines[i] = sc.toString();
        }
    }

    private static String[] strLines;

    private static Set<String> sensorSet = new HashSet<>();

    private static void writeToFile(long spaceLimit) throws InterruptedException, IOException {
        long lineCount = 0;
        long startTime = System.currentTimeMillis();
        long endTime;
        long currentSpace = 0;
        long startTimestamp = System.currentTimeMillis();
        while (currentSpace < spaceLimit) {
            if (lineCount % 1000000 == 0) {
                endTime = System.currentTimeMillis();
                currentSpace =
                        (long) FileUtils.getLocalFileByte(outputDataFile, FileUtils.Unit.B)
                                + writer.calculateMemSizeForAllGroup();
                LOG.info("write line:{},use time:{}s, space:{}", lineCount,
                        (endTime - startTime) / 1000,
                        FileUtils.transformUnit(currentSpace, FileUtils.Unit.MB));

            }
            getNextRecord(startTimestamp + lineCount, lineCount);
            try {

                for (String str : strLines) {
                    TSRecord ts = RecordUtils.parseSimpleTupleRecord(str, fileSchema);
                    writer.write(ts);
                }
            } catch (WriteProcessException e) {
                e.printStackTrace();
            }
            lineCount++;
        }
        writer.close();
        endTime = System.currentTimeMillis();
        LOG.info("write total:{},use time:{}s", lineCount, (endTime - startTime) / 1000);
        LOG.info("src file size:{}MB", FileUtils.getLocalFileByte(outputDataFile, FileUtils.Unit.MB));
    }

    private static JSONObject generateTestSchema() {
        conf = TSFileDescriptor.getInstance().getConfig();
        JSONObject s0 = new JSONObject();
        s0.put(JsonFormatConstant.MEASUREMENT_UID, "s0");
        s0.put(JsonFormatConstant.DATA_TYPE, TSDataType.INT32.toString());
        s0.put(JsonFormatConstant.MEASUREMENT_ENCODING,
                conf.valueEncoder);
        JSONObject s1 = new JSONObject();
        s1.put(JsonFormatConstant.MEASUREMENT_UID, "s1");
        s1.put(JsonFormatConstant.DATA_TYPE, TSDataType.INT64.toString());
        s1.put(JsonFormatConstant.MEASUREMENT_ENCODING,
                conf.valueEncoder);
        JSONObject s2 = new JSONObject();
        s2.put(JsonFormatConstant.MEASUREMENT_UID, "s2");
        s2.put(JsonFormatConstant.DATA_TYPE, TSDataType.FLOAT.toString());
        s2.put(JsonFormatConstant.MEASUREMENT_ENCODING,
                conf.valueEncoder);
        JSONObject s3 = new JSONObject();
        s3.put(JsonFormatConstant.MEASUREMENT_UID, "s3");
        s3.put(JsonFormatConstant.DATA_TYPE, TSDataType.DOUBLE.toString());
        s3.put(JsonFormatConstant.MEASUREMENT_ENCODING,
                conf.valueEncoder);
        JSONObject s4 = new JSONObject();
        s4.put(JsonFormatConstant.MEASUREMENT_UID, "s4");
        s4.put(JsonFormatConstant.DATA_TYPE, TSDataType.TEXT.toString());
        s4.put(JsonFormatConstant.MEASUREMENT_ENCODING,
                TSEncoding.PLAIN.toString());
        JSONArray measureGroup1 = new JSONArray();
        measureGroup1.put(s0);
        measureGroup1.put(s1);
        measureGroup1.put(s2);
        measureGroup1.put(s3);
        measureGroup1.put(s4);

        JSONObject jsonSchema = new JSONObject();
        jsonSchema.put(JsonFormatConstant.DELTA_TYPE, "test_type");
        jsonSchema.put(JsonFormatConstant.JSON_SCHEMA, measureGroup1);
        return jsonSchema;
    }

    private static FileSchema fileSchema;

    public static void main(String[] args) throws IOException, InterruptedException, WriteProcessException {
        if (args.length < 3) {
            System.err.println("input format: <outputFile> <size> <unit> [rowGroupSize(MB)]");
            return;
        }
        outputDataFile = args[0];
        if (new File(outputDataFile).exists())
            new File(outputDataFile).delete();
        fileSchema = new FileSchema(generateTestSchema());
        long size =
                (long) FileUtils
                        .transformUnitToByte(Double.valueOf(args[1]), FileUtils.Unit.valueOf(args[2]));
        if (args.length >= 4)
            setRowGroupSize =
                    (int) FileUtils.transformUnitToByte(Integer.valueOf(args[3]), FileUtils.Unit.MB);
        conf.groupSizeInByte = setRowGroupSize;
        deviceCount = 1;
        strLines = new String[deviceCount];
        sensorSet.add("s0");
        sensorSet.add("s1");
        sensorSet.add("s2");
        // write file
        writer = new TsFileWriter(new File(outputDataFile), fileSchema, conf);
        System.out.println("setRowGroupSize: " + setRowGroupSize + ",total target:" + size);
        writeToFile(size);
    }
}
