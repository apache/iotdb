package cn.edu.tsinghua.iotdb.query.control;

import cn.edu.tsinghua.iotdb.conf.IoTDBConfig;
import cn.edu.tsinghua.iotdb.conf.IoTDBDescriptor;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static org.junit.Assert.fail;


public class FileReaderManagerTest {

    private static final int MAX_FILE_SIZE = 10;

    private IoTDBConfig dbConfig = IoTDBDescriptor.getInstance().getConfig();
    private long cacheFileReaderClearPeriod;

    @Before
    public void setUp() {
        cacheFileReaderClearPeriod = dbConfig.cacheFileReaderClearPeriod;
        dbConfig.cacheFileReaderClearPeriod = 3000;
    }

    @After
    public void tearDown() {
        dbConfig.cacheFileReaderClearPeriod = cacheFileReaderClearPeriod;
    }

    @Test
    public void test() throws IOException, InterruptedException {

        String filePath = "target/test.file";

        FileReaderManager manager = FileReaderManager.getInstance();

        for (int i = 1; i <= MAX_FILE_SIZE; i++) {
            File file = new File(filePath + i);
            file.createNewFile();
        }

        Thread t1 = new Thread(() -> {
            try {
                OpenedFilePathsManager.getInstance().setJobIdForCurrentRequestThread(1L);

                for (int i = 1; i <= 6; i++) {
                    OpenedFilePathsManager.getInstance().addFilePathToMap(1L, filePath + i);
                    manager.get(filePath + i, true);
                    Assert.assertTrue(manager.contains(filePath + i));
                }


            } catch (IOException e) {
                e.printStackTrace();
            }

        });
        t1.start();

        Thread t2 = new Thread(() -> {
            try {
                OpenedFilePathsManager.getInstance().setJobIdForCurrentRequestThread(2L);

                for (int i = 4; i <= MAX_FILE_SIZE; i++) {
                    OpenedFilePathsManager.getInstance().addFilePathToMap(2L, filePath + i);
                    manager.get(filePath + i, true);
                    Assert.assertTrue(manager.contains(filePath + i));
                }

            } catch (IOException e) {
                e.printStackTrace();
            }

        });
        t2.start();

        t1.join();
        t2.join();

        for (int i = 1; i <= MAX_FILE_SIZE; i++) {
            Assert.assertTrue(manager.contains(filePath + i));
        }

        for (int i = 1; i <= MAX_FILE_SIZE; i++) {
            manager.decreaseFileReaderReference(filePath + i);
        }

        // the code below is not valid because the cacheFileReaderClearPeriod config in this class is not valid

//        TimeUnit.SECONDS.sleep(5);
//
//        for (int i = 1; i <= MAX_FILE_SIZE; i++) {
//
//            if (i == 4 || i == 5 || i == 6) {
//                Assert.assertTrue(manager.contains(filePath + i));
//            } else {
//                Assert.assertFalse(manager.contains(filePath + i));
//            }
//        }

        OpenedFilePathsManager.getInstance().removeUsedFilesForCurrentRequestThread();
        FileReaderManager.getInstance().closeAndRemoveAllOpenedReaders();
        for (int i = 1; i < MAX_FILE_SIZE; i++) {
            File file = new File(filePath + i);
            boolean result = Files.deleteIfExists(file.toPath());
            if (!result) {
                fail();
            }
        }
    }
}
