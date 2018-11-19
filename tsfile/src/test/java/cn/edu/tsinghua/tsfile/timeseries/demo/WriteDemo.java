package cn.edu.tsinghua.tsfile.timeseries.demo;

import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;
import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.common.constant.JsonFormatConstant;
import cn.edu.tsinghua.tsfile.common.utils.TsRandomAccessFileWriter;
import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileWriter;
import cn.edu.tsinghua.tsfile.timeseries.utils.FileUtils;
import cn.edu.tsinghua.tsfile.timeseries.utils.RecordUtils;
import cn.edu.tsinghua.tsfile.timeseries.write.TsFileWriter;

import cn.edu.tsinghua.tsfile.timeseries.write.exception.WriteProcessException;
import cn.edu.tsinghua.tsfile.timeseries.write.io.TsFileIOWriter;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;
import cn.edu.tsinghua.tsfile.timeseries.write.schema.FileSchema;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

/**
 * Write Demo provides a JAVA application that receives CSV file and writes to TSfile format.
 * This application requires four parameters (inputDataFilePath,outputDataFilePath, errorFile
 * and schemaFile) and one optional parameter (confFile).
 * Four parameters are needed: inputDataFilePath, outputDataFilePath, errorFile and schemaFile.
 *
 * @author kangrong
 */
public class WriteDemo {
    static final Logger LOG = LoggerFactory.getLogger(WriteDemo.class);
    public static TsFileWriter tsFileWriter;
    public static String inputDataFile;
    public static String outputDataFile;
    public static String errorOutputDataFile;
    public static JSONObject jsonSchema;

    private static void write() throws IOException, InterruptedException, WriteProcessException {
        File file = new File(outputDataFile);
        File errorFile = new File(errorOutputDataFile);
        if (file.exists())
            file.delete();
        if (errorFile.exists())
            errorFile.delete();
        FileSchema schema = new FileSchema(jsonSchema);
        TSFileConfig conf = TSFileDescriptor.getInstance().getConfig();
        tsFileWriter = new TsFileWriter(file, schema, conf);

        // write to file
        try {
            writeToFile(schema);
        } catch (WriteProcessException e) {
            e.printStackTrace();
        }
        LOG.info("write to file successfully!!");
    }

    private static void writeToFile(FileSchema schema) throws InterruptedException, IOException, WriteProcessException {
        BufferedReader br = new BufferedReader(new FileReader(inputDataFile));
        long lineCount = 0;
        long startTime = System.currentTimeMillis();
        long endTime;
        String line;
        while ((line = br.readLine()) != null) {
            if (lineCount % 1000000 == 0) {
                endTime = System.currentTimeMillis();
                LOG.info("write line:{},inner space consumer:{},use time:{}", lineCount,
                        tsFileWriter.calculateMemSizeForAllGroup(), endTime);
                LOG.info("write line:{},use time:{}s", lineCount, (endTime - startTime) / 1000);
            }
            // String str = in.nextLine();
            TSRecord record = RecordUtils.parseSimpleTupleRecord(line, schema);
            tsFileWriter.write(record);
            lineCount++;
        }
        endTime = System.currentTimeMillis();
        LOG.info("write line:{},use time:{}s", lineCount, (endTime - startTime) / 1000);
        tsFileWriter.close();
        endTime = System.currentTimeMillis();
        LOG.info("write total:{},use time:{}s", lineCount, (endTime - startTime) / 1000);
        LOG.info("src file size:{}GB", FileUtils.getLocalFileByte(inputDataFile, FileUtils.Unit.GB));
        LOG.info("src file size:{}MB", FileUtils.getLocalFileByte(outputDataFile, FileUtils.Unit.MB));
        br.close();
    }

    public static void main(String[] args) throws JSONException, IOException, InterruptedException, WriteProcessException {
        if (args.length < 4) {
            LOG.error("\n\ninput args format error, you should run as: " +
                    "<inputDataFilePath> <outputDataFilePath> <errorFile> <schemaFile>\n");
            return;
        }
        inputDataFile = args[0];
        outputDataFile = args[1];
        errorOutputDataFile = args[2];
        System.out.println(args[3]);
        String path = args[3];
        JSONObject obj = new JSONObject(new JSONTokener(new FileReader(new File(path))));
        System.out.println(obj);
        if (!obj.has(JsonFormatConstant.JSON_SCHEMA)) {
            LOG.error("input schema format error");
            return;
        }
        jsonSchema = obj;
        System.out.println(args.length);
        write();
    }
}
