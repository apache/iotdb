package cn.edu.tsinghua.iotdb.performance.index;

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
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class GenBigTsFile {
    private static final Logger LOG = LoggerFactory.getLogger(GenBigTsFile.class);
    private static TsFileWriter writer;
    private static String outputDataFile;
    private static TSFileConfig conf = TSFileDescriptor.getInstance().getConfig();

    private static int setRowGroupSize = conf.groupSizeInByte;
    // To be configure
    private static int deviceCount = 1;
    private static Random random = new Random();
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
    //s5: random walk
    private static int[] startNumbers = {100, 1000, 2000};
    //    private static int[] randomBounds = {2, 10, 100};
    private static int[] randomRadius = {1, 5, 50};
    private static long defaultStartTimestamp;


    private static void getNextRecord(long timestamp, long index) throws IOException {
        for (int i = 0; i < 1; i++) {
            StringContainer sc = new StringContainer(",");
            sc.addTail("root.vehicle.d" + deviceCount, timestamp);
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
            if (sensorSet.contains("s5")) {
                // s3:sin, double
                startNumbers[i] += random.nextBoolean() ? randomRadius[i] : -randomRadius[i];
                sc.addTail("s5", startNumbers[i]);
            }
            strLines[i] = sc.toString();
        }
    }

    private static String[] strLines;

    private static Set<String> sensorSet = new HashSet<>();

    private static void writeToFileByLine(long lineLimit) throws InterruptedException, IOException {
        writeToFile(Long.MAX_VALUE, lineLimit);
    }

    private static void writeToFile(long spaceLimit, long lineLimit) throws InterruptedException, IOException {
        long lineCount = 0;
        long startTimestamp = defaultStartTimestamp;
        long endTime;
        long currentSpace = 0;
        long startTime = System.currentTimeMillis();
        while (currentSpace < spaceLimit && lineCount < lineLimit) {
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
//                    System.out.println(str);
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
        JSONObject s5 = new JSONObject();
        s5.put(JsonFormatConstant.MEASUREMENT_UID, "s5");
        s5.put(JsonFormatConstant.DATA_TYPE, TSDataType.INT32.toString());
        s5.put(JsonFormatConstant.MEASUREMENT_ENCODING,
                TSEncoding.RLE.toString());
        JSONArray measureGroup1 = new JSONArray();
        if (sensorSet.contains("s0"))
            measureGroup1.put(s0);
        if (sensorSet.contains("s1"))
            measureGroup1.put(s1);
        if (sensorSet.contains("s2"))
            measureGroup1.put(s2);
        if (sensorSet.contains("s3"))
            measureGroup1.put(s3);
        if (sensorSet.contains("s4"))
            measureGroup1.put(s4);
        if (sensorSet.contains("s5"))
            measureGroup1.put(s5);

        JSONObject jsonSchema = new JSONObject();
        jsonSchema.put(JsonFormatConstant.DELTA_TYPE, "test_type");
        jsonSchema.put(JsonFormatConstant.JSON_SCHEMA, measureGroup1);
        return jsonSchema;
    }

    private static FileSchema fileSchema;

    public static void generate(long count, String filename, String[] sensors, int dCount, long startTime) throws IOException, InterruptedException, WriteProcessException {
        long lineLimit = count;
        outputDataFile = filename;
        if (new File(outputDataFile).exists())
            new File(outputDataFile).delete();
        sensorSet.clear();
        for (String sr : sensors) {
            sensorSet.add(sr);
        }
        fileSchema = new FileSchema(generateTestSchema());
        conf.groupSizeInByte = setRowGroupSize;
        deviceCount = dCount;
        strLines = new String[1];
        // write file
        defaultStartTimestamp = startTime;
        writer = new TsFileWriter(new File(outputDataFile), fileSchema, conf);
        System.out.println("setRowGroupSize: " + setRowGroupSize + ",total target line:" + lineLimit);
        writeToFileByLine(lineLimit);
    }
}
