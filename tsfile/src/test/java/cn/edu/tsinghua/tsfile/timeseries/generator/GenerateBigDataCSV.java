package cn.edu.tsinghua.tsfile.timeseries.generator;

import cn.edu.tsinghua.tsfile.common.constant.JsonFormatConstant;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.tsinghua.tsfile.timeseries.utils.StringContainer;
import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;
import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class GenerateBigDataCSV {
    private static final Logger LOG = LoggerFactory.getLogger(GenerateBigDataCSV.class);
    private static String inputDataFile;

    // private String[] deviceList;
    // To be configure
    private static int deviceCount = 3;
    // s0:broken line
    // s1:line
    // s2:square wave with frequency noise
    // s3:long for sin with glitch
    // s4:log
    private static int[][] brokenLineConfigs = { {1000, 1, 100}, {200000, 5, 200000},
            {10000, 2, 50000}};
    private static long[][] lineConfigs = { {1L << 32, 1}, {1L << 22, 4}, {10000, 2}};
    private static float[] squareAmplitude = {12.5f, 1273.143f, 1823767.4f};
    private static float[] squareBaseLine = {25f, 2273.143f, 2823767.4f};
    private static int[] squareLength = {300, 5000, 20000};
    private static double[][] sinAbnormalConfigs = { {0.28, 20}, {0.3, 100}, {0.35, 50}};
    // y = A*sin(wt), sinConfigs:w,A
    private static double[][] sinConfigs = { {0.05, 2000}, {0.03, 1000}, {0.001, 200}};
    private static int[][] maxMinVertical = { {5000, 4000}, {1000, 800}, {100, 70}};
    private static int[][] maxMinHorizontal = { {5, 2}, {20, 10}, {50, 30}};
    private static double[] glitchProbability = {0.008, 0.01, 0.005};

    private static String deltaObjectType = "root.laptop";

    private static float freqWave[] = {0, 0, 0};

    private static void getNextRecordToFile(long timestamp, long index, FileWriter fw) throws IOException {
        for (int i = 0; i < deviceCount; i++) {
            StringContainer sc = new StringContainer(",");
            sc.addTail("d" + i, timestamp+index, deltaObjectType);
            if (sensorSet.contains("s0")) {
                // s0:broken line, int
                if ((index % brokenLineConfigs[i][2]) == 0)
                    brokenLineConfigs[i][1] = -brokenLineConfigs[i][1];
                brokenLineConfigs[i][0] += brokenLineConfigs[i][1];
                if (brokenLineConfigs[i][0] < 0) {
                    brokenLineConfigs[i][0] = -brokenLineConfigs[i][0];
                    brokenLineConfigs[i][1] = -brokenLineConfigs[i][1];
                }
                sc.addTail("s0", brokenLineConfigs[i][0]);
            }
            if (sensorSet.contains("s1")) {
                // s1:line, long
                lineConfigs[i][0] += lineConfigs[i][1];
                if (lineConfigs[i][0] < 0)
                    lineConfigs[i][0] = 0;
                sc.addTail("s1", lineConfigs[i][0]);
            }
            if (sensorSet.contains("s2")) {// s2:square wave, float
                if ((index % squareLength[i]) == 0) {
                    squareAmplitude[i] = -squareAmplitude[i];
                    if (hasWrittenFreq[i] == 0) {
                        if ((double) index == squareLength[i]) {
                            System.out.println("d"+i+":time:"+index+",sin sin");
                            hasWrittenFreq[i] = 1;
                        }
                    } else if (hasWrittenFreq[i] == 1) {
                        hasWrittenFreq[i] = 2;
                    }
                }
                freqWave[i] =
                        (hasWrittenFreq[i] == 1) ? (float) (squareAmplitude[i] / 2 * Math
                                .sin(sinAbnormalConfigs[i][0] * 2 * Math.PI * index)) : 0;
                sc.addTail("s2", freqWave[i] + squareBaseLine[i] + squareAmplitude[i]);
            }
            if (sensorSet.contains("s3")) {
                // s3:sin, long
                sc.addTail("s3", generateSinGlitch(timestamp+index, i));
            }
            fw.write(sc.toString() + "\r\n");
        }
    }

    private static Random r = new Random();
    private static int[] width = {-1, -1, -1};
    private static int[] mid = {0, 0, 0};
    private static long[] upPeek = {0, 0, 0};
    private static long[] downPeek = {0, 0, 0};
    private static long[] base = {0, 0, 0};
    private static long[] startAbTime = {0, 0, 0};

    private static long generateSinGlitch(long t, int i) {
        if (r.nextDouble() < glitchProbability[i] && width[i] == -1) {
            startAbTime[i] = t;
            base[i] =
                    (long) (maxMinVertical[i][0] + sinConfigs[i][1] + sinConfigs[i][1]
                            * Math.sin(sinConfigs[i][0] * t));
            width[i] =
                    r.nextInt(maxMinHorizontal[i][0] - maxMinHorizontal[i][1])
                            + maxMinHorizontal[i][1];

            if (width[i] < 2)
                width[i] = 2;
            mid[i] = r.nextInt(width[i] - 1) + 1;
            upPeek[i] =
                    maxMinVertical[i][1] + r.nextInt(maxMinVertical[i][0] - maxMinVertical[i][1]);
            downPeek[i] =
                    maxMinVertical[i][1] + r.nextInt(maxMinVertical[i][0] - maxMinVertical[i][1]);
            return base[i];
        } else {
            if (width[i] != -1) {
                long value;
                // up
                if (t - startAbTime[i] <= mid[i]) {
                    value = (long) (base[i] + ((double) t - startAbTime[i]) / mid[i] * upPeek[i]);
                } else {
                    value =
                            (long) (base[i] + upPeek[i] - ((double) t - mid[i] - startAbTime[i])
                                    / (width[i] - mid[i]) * downPeek[i]);
                }
                if (t - startAbTime[i] == width[i])
                    width[i] = -1;
                // down
                return value;
            } else {
                return (long) (maxMinVertical[i][0] + sinConfigs[i][1] + sinConfigs[i][1]
                        * Math.sin(sinConfigs[i][0] * t));
            }
        }
    }

    private static JSONObject generateTestDataSchema(String schemaOutputFilePath) throws IOException {
        TSFileConfig conf = TSFileDescriptor.getInstance().getConfig();
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
        File file = new File(schemaOutputFilePath);
        FileWriter fw = new FileWriter(file);
        fw.write(jsonSchema.toString());
        fw.close();
        return jsonSchema;
    }

    private static Set<String> sensorSet = new HashSet<>();
    private static long lineCount;
    private static double writeFreqFraction[] = {0, 0, 0};
    // 0:not write->1:writing->2:written
    private static int hasWrittenFreq[] = {0, 0, 0};

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length < 4) {
            System.err.println("sensorName:s0:int,s1:long,s2:float,s3:long sin");
            System.err
                    .println("input format: <csvFilePath> <schemaOutputFilePath> <lineCount> <sensorName> [<sensorName>...]");
            return;
        }
        LOG.info("write start!");
        inputDataFile = args[0];
        String schemaOutputFilePath = args[1];
        lineCount = Long.valueOf(args[2]);
        sensorSet.addAll(Arrays.asList(args).subList(3, args.length));
        generateTestDataSchema(schemaOutputFilePath);
        int i = 0;
        File file = new File(inputDataFile);
        if (file.exists())
            file.delete();
        FileWriter fw = new FileWriter(file);
        deviceCount = 3;
        for (int j = 0; j < deviceCount; j++) {
            writeFreqFraction[i] = r.nextDouble();
        }

        long start = System.currentTimeMillis();
        // TODO to be changed
        // start = 0;
        while (i < lineCount) {
            if (i % 1000000 == 0) {
                LOG.info("generate line count:{}", i);
            }
            getNextRecordToFile(start,i, fw);
            i++;
        }
        fw.close();
        LOG.info("write finished!");
    }
}
