package cn.edu.tsinghua.iotdb.utils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.nio.file.Files;
import java.util.ArrayList;
import org.apache.commons.io.FileUtils;

import static org.junit.Assert.assertEquals;

public class OpenFileNumUtilTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(OpenFileNumUtilTest.class);
    private OpenFileNumUtil openFileNumUtil = OpenFileNumUtil.getInstance();
    private ArrayList<File> fileList = new ArrayList<>();
    private ArrayList<FileWriter> fileWriterList = new ArrayList<>();
    private String testFileName;
    private static final String TEST_FILE_PREFIX = "testFileForOpenFileNumUtil";
    private static final String MAC_OS_NAME = "mac";
    private static final String LINUX_OS_NAME = "linux";
    private int totalOpenFileNumBefore;
    private int totalOpenFileNumAfter;
    private int totalOpenFileNumChange;
    private int testFileNum = 66;
    private String currDir;
    private File testDataDirRoot;
    private String os = System.getProperty("os.name").toLowerCase();

    @Before
    public void setUp() {
        int testProcessID = getProcessID();
        openFileNumUtil.setPid(testProcessID);
        String dataFilePath = OpenFileNumUtil.OpenFileNumStatistics.DATA_OPEN_FILE_NUM.getPath().get(0);
        String userDir = System.getProperty("user.dir");
        currDir = userDir + File.separator + testProcessID + File.separator + dataFilePath;
        File testDataDir = new File(currDir);
        testDataDirRoot = new File(userDir + File.separator + testProcessID);
        if(!testDataDir.exists()) {
            if (!testDataDir.isDirectory()) {
                if (!testDataDir.mkdirs()) {
                    LOGGER.error("Create test file dir {} failed.", testDataDir.getPath());
                }
            }
        }
        testFileName = TEST_FILE_PREFIX + testProcessID;
    }

    @After
    public void tearDown() {
        //close FileWriter
        for (FileWriter fw : fileWriterList) {
            try {
                fw.close();
            } catch (IOException e) {
                LOGGER.error(e.getMessage());
            }
        }

        //delete test files
        for (File file : fileList) {
            if (file.exists()) {
                try {
                    Files.delete(file.toPath());
                } catch (IOException e) {
                    LOGGER.error(e.getMessage());
                }
            }
        }
        fileWriterList.clear();
        fileList.clear();
        try {
            FileUtils.deleteDirectory(testDataDirRoot);
        } catch (IOException e) {
            LOGGER.error("Delete test data dir {} failed.", testDataDirRoot);
        }
    }

    @Test
    public void testDataOpenFileNumWhenCreateFile() {
        if(os.startsWith(MAC_OS_NAME) || os.startsWith(LINUX_OS_NAME)) {
            //get total open file number statistics of original state
            totalOpenFileNumBefore = openFileNumUtil.get(OpenFileNumUtil.OpenFileNumStatistics.DATA_OPEN_FILE_NUM);
            for (int i = 0; i < testFileNum; i++) {
                fileList.add(new File(currDir + testFileName + i));
            }
            //create testFileNum File, then get total open file number statistics
            totalOpenFileNumAfter = openFileNumUtil.get(OpenFileNumUtil.OpenFileNumStatistics.DATA_OPEN_FILE_NUM);
            totalOpenFileNumChange = totalOpenFileNumAfter - totalOpenFileNumBefore;
            //create test file shall not affect total open file number statistics
            assertEquals(0, totalOpenFileNumChange);
        }else {
            assertEquals(-2, openFileNumUtil.get(OpenFileNumUtil.OpenFileNumStatistics.DATA_OPEN_FILE_NUM));
        }
    }

    @Test
    public void testDataOpenFileNumWhenCreateFileWriter() {
        if(os.startsWith(MAC_OS_NAME) || os.startsWith(LINUX_OS_NAME)) {
            for (int i = 0; i < testFileNum; i++) {
                fileList.add(new File(currDir + testFileName + i));
            }
            totalOpenFileNumBefore = openFileNumUtil.get(OpenFileNumUtil.OpenFileNumStatistics.DATA_OPEN_FILE_NUM);
            for (File file : fileList) {
                if (file.exists()) {
                    try {
                        fileWriterList.add(new FileWriter(file));
                    } catch (IOException e) {
                        LOGGER.error(e.getMessage());
                    }
                } else {
                    try {
                        boolean flag = file.createNewFile();
                        if(!flag){
                            LOGGER.error("create test file {} failed when execute testTotalOpenFileNumWhenCreateFileWriter().", file.getPath());
                        }
                    } catch (IOException e) {
                        LOGGER.error(e.getMessage());
                    }
                    try {
                        fileWriterList.add(new FileWriter(file));
                    } catch (IOException e) {
                        LOGGER.error(e.getMessage());
                    }
                }
            }
            totalOpenFileNumAfter = openFileNumUtil.get(OpenFileNumUtil.OpenFileNumStatistics.DATA_OPEN_FILE_NUM);
            totalOpenFileNumChange = totalOpenFileNumAfter - totalOpenFileNumBefore;
            //create FileWriter shall cause total open file number increase by testFileNum
            assertEquals(testFileNum, totalOpenFileNumChange);
        } else {
            assertEquals(-2, openFileNumUtil.get(OpenFileNumUtil.OpenFileNumStatistics.DATA_OPEN_FILE_NUM));
        }
    }

    @Test
    public void testDataOpenFileNumWhenFileWriterWriting() {
        if(os.startsWith(MAC_OS_NAME) || os.startsWith(LINUX_OS_NAME)) {
            for (int i = 0; i < testFileNum; i++) {
                fileList.add(new File(currDir + testFileName + i));
            }
            for (File file : fileList) {
                if (file.exists()) {
                    try {
                        fileWriterList.add(new FileWriter(file));
                    } catch (IOException e) {
                        LOGGER.error(e.getMessage());
                    }
                } else {
                    try {
                        if(!file.createNewFile()){
                            LOGGER.error("create test file {} failed.", file.getPath());
                        }
                    } catch (IOException e) {
                        LOGGER.error(e.getMessage());
                    }
                    try {
                        fileWriterList.add(new FileWriter(file));
                    } catch (IOException e) {
                        LOGGER.error(e.getMessage());
                    }
                }
            }
            totalOpenFileNumBefore = openFileNumUtil.get(OpenFileNumUtil.OpenFileNumStatistics.DATA_OPEN_FILE_NUM);
            for (FileWriter fw : fileWriterList) {
                try {
                    fw.write("this is a test file for open file number counting.");
                } catch (IOException e) {
                    LOGGER.error(e.getMessage());
                }
            }
            totalOpenFileNumAfter = openFileNumUtil.get(OpenFileNumUtil.OpenFileNumStatistics.DATA_OPEN_FILE_NUM);
            totalOpenFileNumChange = totalOpenFileNumAfter - totalOpenFileNumBefore;
            //writing test file shall not affect total open file number statistics
            assertEquals(0, totalOpenFileNumChange);
        } else {
            assertEquals(-2, openFileNumUtil.get(OpenFileNumUtil.OpenFileNumStatistics.DATA_OPEN_FILE_NUM));
        }
    }

    @Test
    public void testDataOpenFileNumWhenFileWriterClose() {
        if(os.startsWith(MAC_OS_NAME) || os.startsWith(LINUX_OS_NAME)) {
            for (int i = 0; i < testFileNum; i++) {
                fileList.add(new File(currDir + testFileName + i));
            }
            for (File file : fileList) {
                if (file.exists()) {
                    try {
                        fileWriterList.add(new FileWriter(file));
                    } catch (IOException e) {
                        LOGGER.error(e.getMessage());
                    }
                } else {
                    try {
                        if(!file.createNewFile()){
                            LOGGER.error("create test file {} failed when execute testTotalOpenFileNumWhenFileWriterClose().", file.getPath());
                        }
                    } catch (IOException e) {
                        LOGGER.error(e.getMessage());
                    }
                    try {
                        fileWriterList.add(new FileWriter(file));
                    } catch (IOException e) {
                        LOGGER.error(e.getMessage());
                    }
                }
            }
            for (FileWriter fw : fileWriterList) {
                try {
                    fw.write("this is a test file for open file number counting.");
                } catch (IOException e) {
                    LOGGER.error(e.getMessage());
                }
            }
            totalOpenFileNumBefore = openFileNumUtil.get(OpenFileNumUtil.OpenFileNumStatistics.DATA_OPEN_FILE_NUM);
            for (FileWriter fw : fileWriterList) {
                try {
                    fw.close();
                } catch (IOException e) {
                    LOGGER.error(e.getMessage());
                }
            }
            totalOpenFileNumAfter = openFileNumUtil.get(OpenFileNumUtil.OpenFileNumStatistics.DATA_OPEN_FILE_NUM);
            totalOpenFileNumChange = totalOpenFileNumAfter - totalOpenFileNumBefore;
            //close FileWriter shall cause total open file number decrease by testFileNum
            assertEquals(-testFileNum, totalOpenFileNumChange);
        } else {
            assertEquals(-2, openFileNumUtil.get(OpenFileNumUtil.OpenFileNumStatistics.DATA_OPEN_FILE_NUM));
        }
    }


    private static int getProcessID() {
        RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
        return Integer.parseInt(runtimeMXBean.getName().split("@")[0]);
    }

}
