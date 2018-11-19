package cn.edu.tsinghua.tsfile.timeseries;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.json.JSONObject;
import org.json.JSONTokener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.tsfile.timeseries.basis.TsFile;
import cn.edu.tsinghua.tsfile.timeseries.utils.FileUtils;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.WriteProcessException;

/**
 * Created by kangrong on 17/3/27.
 */
public class TsFileTest {
    private static final Logger LOG = LoggerFactory.getLogger(TsFileTest.class);

    /**
     * TO be deleted
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException, WriteProcessException {
        args = new String[]{
                "/Volumes/KINGSTON/2_nc.csv",
                "/Users/kangrong/out.tsfile",
                "/Volumes/KINGSTON/4_new_nc.json"
        };
        String inputCSV = args[0];
        String outputFilePath = args[1];
        String schemaJsonPath = args[2];
        JSONObject schemaObj = new JSONObject(new JSONTokener(new FileReader(new File(schemaJsonPath))));
        File outputFile = new File(outputFilePath);
        if(outputFile.exists())
            outputFile.delete();
        TsFile tsfile = new TsFile(outputFile, schemaObj);
        //write
        BufferedReader br = new BufferedReader(new FileReader(inputCSV));
        long lineCount = 0;
        long startTime = System.currentTimeMillis();
        long endTime;
        String line;
        while ((line = br.readLine()) != null) {
            if(lineCount > 100000)
                break;
            if (lineCount % 1000000 == 0) {
                endTime = System.currentTimeMillis();
                LOG.info("write line:{},use time:{}s", lineCount, (endTime - startTime) / 1000);
            }
            try {
                tsfile.writeLine(line);
            } catch (Exception e) {
                e.printStackTrace();
            }
            lineCount++;
        }
        endTime = System.currentTimeMillis();
        LOG.info("write line:{},use time:{}s", lineCount, (endTime - startTime) / 1000);
        tsfile.close();
        endTime = System.currentTimeMillis();
        LOG.info("write total:{},use time:{}s", lineCount, (endTime - startTime) / 1000);
        LOG.info("src file size:{}GB", FileUtils.getLocalFileByte(inputCSV, FileUtils.Unit.GB));
        LOG.info("src file size:{}MB", FileUtils.getLocalFileByte(outputFilePath, FileUtils.Unit.MB));
        br.close();
    }
}