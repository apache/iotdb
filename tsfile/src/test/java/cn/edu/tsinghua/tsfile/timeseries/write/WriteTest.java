package cn.edu.tsinghua.tsfile.timeseries.write;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;
import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.common.constant.JsonFormatConstant;
import cn.edu.tsinghua.tsfile.timeseries.basis.TsFile;
import cn.edu.tsinghua.tsfile.timeseries.read.TsRandomAccessLocalFileReader;
import cn.edu.tsinghua.tsfile.timeseries.utils.RecordUtils;
import cn.edu.tsinghua.tsfile.timeseries.utils.StringContainer;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.WriteProcessException;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;
import cn.edu.tsinghua.tsfile.timeseries.write.schema.FileSchema;


/**
 * test writing processing correction combining writing process and reading process.
 *
 * @author kangrong
 */
public class WriteTest {
    private static final Logger LOG = LoggerFactory.getLogger(WriteTest.class);
    private final int ROW_COUNT = 20;
    private TsFileWriter tsFileWriter;
    private String inputDataFile;
    private String outputDataFile;
    private String errorOutputDataFile;
    private String schemaFile;
    private Random rm = new Random();
    private FileSchema schema;
    private int stageSize = 4;
    private int stageState = -1;
    private int prePageSize;
    private int prePageCheckThres;
    private TSFileConfig conf = TSFileDescriptor.getInstance().getConfig();
    private JSONArray measurementArray;

    @Before
    public void prepare() throws IOException, WriteProcessException {
        inputDataFile = "src/test/resources/writeTestInputData";
        outputDataFile = "src/test/resources/writeTestOutputData.ksn";
        errorOutputDataFile = "src/test/resources/writeTestErrorOutputData.ksn";
        schemaFile = "src/test/resources/test_write_schema.json";
        // for each row, flush page forcely
        prePageSize = conf.pageSizeInByte;
        conf.pageSizeInByte = 0;
        prePageCheckThres = conf.pageCheckSizeThreshold;
        conf.pageCheckSizeThreshold = 0;

        try {
            generateSampleInputDataFile();
        } catch (IOException e) {
            fail();
        }
        File file = new File(outputDataFile);
        File errorFile = new File(errorOutputDataFile);
        if (file.exists())
            file.delete();
        if (errorFile.exists())
            errorFile.delete();
        JSONObject emptySchema = new JSONObject(
                "{\"delta_type\": \"test_type\",\"properties\": {\n" +
                        "\"key1\": \"value1\",\n" +
                        "\"key2\": \"value2\"\n" +
                        "},\"schema\": [],}");
        measurementArray =
                new JSONObject(new JSONTokener(new FileReader(new File(schemaFile)))).getJSONArray(JsonFormatConstant
                        .JSON_SCHEMA);
        schema = new FileSchema(emptySchema);
        LOG.info(schema.toString());
        tsFileWriter = new TsFileWriter(file, schema, conf);
    }

    @After
    public void after() {
        File file = new File(inputDataFile);
        if (file.exists())
            file.delete();
        file = new File(outputDataFile);
        if (file.exists())
            file.delete();
        file = new File(errorOutputDataFile);
        if (file.exists())
            file.delete();
    }

    @After
    public void end() {
        conf.pageSizeInByte = prePageSize;
        conf.pageCheckSizeThreshold = prePageCheckThres;
    }

    private void generateSampleInputDataFile() throws IOException {
        File file = new File(inputDataFile);
        if (file.exists())
            file.delete();
        FileWriter fw = new FileWriter(file);

        long startTime = System.currentTimeMillis();
        startTime = startTime - startTime % 1000;

        // first stage:int, long, float, double, boolean, enums
        for (int i = 0; i < ROW_COUNT; i++) {
            // write d1
            String d1 = "d1," + (startTime + i) + ",s1," + (i * 10 + 1) + ",s2," + (i * 10 + 2);
            if (rm.nextInt(1000) < 100) {
                d1 = "d1," + (startTime + i) + ",s1,,s2," + (i * 10 + 2) + ",s4,HIGH";
            }
            if (i % 5 == 0)
                d1 += ",s3," + (i * 10 + 3);
            fw.write(d1 + "\r\n");

            // write d2
            String d2 = "d2," + (startTime + i) + ",s2," + (i * 10 + 2) + ",s3," + (i * 10 + 3);
            if (rm.nextInt(1000) < 100) {
                d2 = "d2," + (startTime + i) + ",s2,,s3," + (i * 10 + 3) + ",s5,MAN";
            }
            if (i % 5 == 0)
                d2 += ",s1," + (i * 10 + 1);
            fw.write(d2 + "\r\n");
        }
        // write error
        String d =
                "d2,3," + (startTime + ROW_COUNT) + ",s2," + (ROW_COUNT * 10 + 2) + ",s3,"
                        + (ROW_COUNT * 10 + 3);
        fw.write(d + "\r\n");
        d = "d2," + (startTime + ROW_COUNT + 1) + ",2,s-1," + (ROW_COUNT * 10 + 2);
        fw.write(d + "\r\n");
        fw.close();
    }

    @Test
    public void writeTest() throws IOException, InterruptedException {
        try {
            write();
        } catch (WriteProcessException e) {
            e.printStackTrace();
        }
        LOG.info("write processing has finished");

        TsRandomAccessLocalFileReader input = new TsRandomAccessLocalFileReader(outputDataFile);
        TsFile readTsFile = new TsFile(input);
        String value1 = readTsFile.getProp("key1");
        Assert.assertEquals("value1", value1);
        String value2 = readTsFile.getProp("key2");
        Assert.assertEquals("value2", value2);
    }

    public void write() throws IOException, WriteProcessException {
        long lineCount = 0;
        long startTime = System.currentTimeMillis();
        String[] strings;
        //add all measurement except the last one at before writing
        for (int i = 0; i < measurementArray.length() - 1; i++) {
            tsFileWriter.addMeasurementByJson((JSONObject) measurementArray.get(i));
        }
        while (true) {
            if (lineCount % stageSize == 0) {
                LOG.info("write line:{},use time:{}s", lineCount,
                        (System.currentTimeMillis() - startTime) / 1000);
                stageState++;
                LOG.info("stage:" + stageState);
                if (stageState == stageDeltaObjectIds.length)
                    break;
            }
            if (lineCount == ROW_COUNT / 2)
                tsFileWriter.addMeasurementByJson((JSONObject) measurementArray.get(measurementArray.length() - 1));
            strings = getNextRecord(lineCount, stageState);
            for (String str : strings) {
                TSRecord record = RecordUtils.parseSimpleTupleRecord(str, schema);
                System.out.println(str);
                tsFileWriter.write(record);
            }
            lineCount++;
        }
        //test duplicate measurement adding
        JSONObject dupMeasure = (JSONObject) measurementArray.get(measurementArray.length() - 1);
        try {
          tsFileWriter.addMeasurementByJson(dupMeasure);
        }catch (WriteProcessException e){
            assertEquals("given measurement has exists! "+
                    dupMeasure.getString(JsonFormatConstant.MEASUREMENT_UID), e.getMessage());
        }
        try {
            tsFileWriter.close();
        } catch (IOException e) {
            fail("close writer failed");
        }
        LOG.info("stage size: {}, write {} group data", stageSize, lineCount);
    }

    private String[][] stageDeltaObjectIds = {{"d1", "d2", "d3"}, {"d1"}, {"d2", "d3"}};
    private String[] measurementIds = {"s0", "s1", "s2", "s3", "s4", "s5"};
    private long longBase = System.currentTimeMillis() * 1000;
    private String[] enums = {"MAN", "WOMAN"};

    private String[] getNextRecord(long lineCount, int stage) {

        String[] ret = new String[stageDeltaObjectIds[stage].length];
        for (int i = 0; i < ret.length; i++) {
            StringContainer sc = new StringContainer(JsonFormatConstant.TSRECORD_SEPARATOR);
            sc.addTail(stageDeltaObjectIds[stage][i], lineCount);
            sc.addTail(measurementIds[0], lineCount * 10 + i, measurementIds[1], longBase
                            + lineCount * 20 + i, measurementIds[2], (lineCount * 30 + i) / 3.0,
                    measurementIds[3], (longBase + lineCount * 40 + i) / 7.0);
            sc.addTail(measurementIds[4], ((lineCount + i) & 1) == 0);
            sc.addTail(measurementIds[5], enums[(int) (lineCount + i) % enums.length]);
            ret[i] = sc.toString();
        }
        return ret;
    }
}
