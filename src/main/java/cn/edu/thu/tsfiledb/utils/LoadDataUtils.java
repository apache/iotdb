package cn.edu.thu.tsfiledb.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfile.timeseries.utils.FileUtils;
import cn.edu.thu.tsfile.timeseries.utils.RecordUtils;
import cn.edu.thu.tsfile.timeseries.write.record.TSRecord;
import cn.edu.thu.tsfile.timeseries.write.schema.FileSchema;
import cn.edu.thu.tsfiledb.conf.TsfileDBConfig;
import cn.edu.thu.tsfiledb.conf.TsfileDBDescriptor;
import cn.edu.thu.tsfiledb.engine.exception.FileNodeManagerException;
import cn.edu.thu.tsfiledb.engine.filenode.FileNodeManager;
import cn.edu.thu.tsfiledb.exception.PathErrorException;
import cn.edu.thu.tsfiledb.metadata.ColumnSchema;
import cn.edu.thu.tsfiledb.metadata.MManager;

import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author kangrong
 */
public class LoadDataUtils {
    private static Logger LOG = LoggerFactory.getLogger(LoadDataUtils.class);
    private BufferedReader inputCsvFileReader;
    private BufferedWriter extraDataFileWriter;
    private FileSchema fileSchema;
    private Set<String> writeInstanceMap;
    private MManager mManager;
    private int writeInstanceThreshold;
    private boolean hasExtra = false;
    private String measureType;
    private long totalPointCount = 0;
    private FileNodeManager fileNodeManager;
    private TsfileDBConfig conf = TsfileDBDescriptor.getInstance().getConfig();

    public LoadDataUtils() {
        writeInstanceMap = new HashSet<>();
        fileNodeManager = FileNodeManager.getInstance();
        writeInstanceThreshold = conf.writeInstanceThreshold;
    }

    /**
     * @param inputCsvDataPath - path
     * @return - return extra data file in this circle as input csv path in next circle
     */
    private String loadLocalDataOnePass(String inputCsvDataPath) {
        hasExtra = false;
        // prepare file for extra data
        String extraDataFilePath = prepareFilePathAddOne(inputCsvDataPath);
        File extraDataFile = new File(extraDataFilePath);
        try {
            this.extraDataFileWriter = new BufferedWriter(new FileWriter(extraDataFile));
        } catch (IOException e) {
            LOG.error("create");
            e.printStackTrace();
            close();
            return null;
        }
        // prepare input csv data file.
        try {
            this.inputCsvFileReader = new BufferedReader(new FileReader(inputCsvDataPath));
        } catch (FileNotFoundException e1) {
            LOG.error("inputCsvDataPath:{} not found!", inputCsvDataPath);
            close();
            return null;
        }
        // load data for each line
        long lineCount = 0;
        long startTime = System.currentTimeMillis();
        long temp = System.currentTimeMillis();
        String line;
        try {
            while ((line = inputCsvFileReader.readLine()) != null) {
                if (lineCount % 1000000 == 0) {
                    long endTime = System.currentTimeMillis();
                    LOG.info("write line:{}, use time:{}", lineCount,
                            (endTime - temp));
                    temp = System.currentTimeMillis();
                    LOG.info("load data points:{}, load data speed:{}w point/s",
                            totalPointCount, FileUtils.format(((float) totalPointCount / 10) / (endTime - startTime), 2));
                }
                loadOneRecordLine(line);
                lineCount++;
            }
        } catch (IOException e1) {
            LOG.error("read line from inputCsvFileReader failed:{}", inputCsvDataPath);
            extraDataFilePath = null;
        } finally {
            LOG.info("write line:{}", lineCount);
            close();
            closeWriteInstance();
        }
        return extraDataFilePath;
    }

    @SuppressWarnings("finally")
    private void loadOneRecordLine(String line) {
        TSRecord record = RecordUtils.parseSimpleTupleRecord(line, this.fileSchema);
        totalPointCount += record.dataPointList.size();
        String nsPath = null;
        try {
            nsPath = mManager.getFileNameByPath(record.deltaObjectId);
        } catch (PathErrorException e) {
            LOG.error("given path not found.{}", e.getMessage());
        }
        if (!writeInstanceMap.contains(nsPath)) {
            if (writeInstanceMap.size() < writeInstanceThreshold) {
                writeInstanceMap.add(nsPath);
            } else {
                hasExtra = true;
                try {
                    extraDataFileWriter.write(line);
                    extraDataFileWriter.newLine();
                } catch (IOException e) {
                    LOG.error("record the extra data into extraFile failed, record:{}", line);
                }
            }
        }
        // appeared before, insert directly
        try {
            fileNodeManager.insert(record);
        } catch (FileNodeManagerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

    private String prepareFilePathAddOne(String srcFilePath) {
        String extraExt = "deltaTempExt";
        int srcEnd = srcFilePath.indexOf(extraExt);
        if (srcEnd != -1)
            srcFilePath = srcFilePath.substring(0, srcEnd);
        File file;
        int ext = 0;
        String tempFile = srcFilePath;
        while (true) {
            file = new File(tempFile);
            if (file.exists())
                tempFile = srcFilePath + extraExt + (ext++);
            else
                break;
        }
        return tempFile;
    }


    private void close() {
        try {
            if (inputCsvFileReader != null)
                inputCsvFileReader.close();
            if (extraDataFileWriter != null)
                extraDataFileWriter.close();
        } catch (IOException e) {
            LOG.error("close inputCsvFileReader and extraDataFileWriter failed");
        }
    }

    private void closeWriteInstance() {
        //TODO for data load, don't close the write processor
//        for (Entry<String, WriteInstance> entry : writeInstanceMap.entrySet()) {
//            entry.getValue().close();
//            LOG.info("closed write instance:{}", entry.getKey());
//        }
        writeInstanceMap.clear();
    }


    public void loadLocalDataMultiPass(String inputCsvDataPath, String measureType,
                                       MManager mManager) {
        LOG.info("start loading data...");
        long start = System.currentTimeMillis();
        System.out.println("asdaasd");
        System.out.println("asdaasd");
        System.out.println("asdaasd");
        System.out.println("asdaasd");
        long startTime = System.currentTimeMillis();
        this.mManager = mManager;
        // get measurement schema
        try {
            ArrayList<ColumnSchema> meaSchema = mManager.getSchemaForOneType(measureType);
            this.measureType = measureType;
            fileSchema = FileSchemaUtil.getFileSchemaFromColumnSchema(meaSchema, measureType);
        } catch (PathErrorException e) {
            LOG.error("the path of input measurement schema meet error!");
            e.printStackTrace();
            close();
            return;
        }
        String extraPath = inputCsvDataPath;
        List<String> extraPaths = new ArrayList<>();
        do {
            LOG.info("cycle: write csv file: {}", extraPath);
            extraPath = loadLocalDataOnePass(extraPath);
            extraPaths.add(extraPath);
        } while (hasExtra);
        for (String ext : extraPaths) {
            LOG.info("delete old file:{}", ext);
            new File(ext).delete();
        }
        long endTime = System.currentTimeMillis();
        LOG.info("load data successfully! total data points:{}, load data speed:{}w point/s",
                totalPointCount, FileUtils.format(((float) totalPointCount / 10) / (endTime - startTime), 2));
    }
}
