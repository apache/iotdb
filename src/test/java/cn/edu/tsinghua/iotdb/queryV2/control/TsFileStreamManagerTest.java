package cn.edu.tsinghua.iotdb.queryV2.control;

import cn.edu.tsinghua.iotdb.queryV2.SimpleFileWriter;
import cn.edu.tsinghua.iotdb.queryV2.engine.component.job.QueryJob;
import cn.edu.tsinghua.iotdb.queryV2.engine.component.job.QueryJob.*;
import cn.edu.tsinghua.iotdb.queryV2.engine.control.TsFileStreamManager;
import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Created by zhangjinrui on 2018/1/14.
 */
public class TsFileStreamManagerTest {

    private static final String PATH = "fileStreamManagerTestFile";
    private TsFileStreamManager fileStreamManager;

    @Before
    public void before() throws IOException {
        SimpleFileWriter.writeFile(10000, PATH);
        fileStreamManager = TsFileStreamManager.getInstance();
    }

    @After
    public void after() {
        File file = new File(PATH);
        if (file.exists()) {
            file.delete();
        }
    }

    @Test
    public void testOneThread() throws IOException {
        QueryJob queryJob1 = new SelectQueryJob(1L);
        ITsRandomAccessFileReader randomAccessFileReader = fileStreamManager.getTsFileStreamReader(queryJob1, PATH);
        ITsRandomAccessFileReader randomAccessFileReader2 = fileStreamManager.getTsFileStreamReader(queryJob1, PATH);
        fileStreamManager.release(queryJob1, PATH);

        try {
            randomAccessFileReader.read();
            fileStreamManager.release(queryJob1, PATH);
            randomAccessFileReader2.read();
            Assert.fail();
        } catch (IOException e) {
            Assert.assertEquals("Stream Closed", e.getMessage());
        }

        ITsRandomAccessFileReader randomAccessFileReader3 = fileStreamManager.getTsFileStreamReader(queryJob1, PATH);
        ITsRandomAccessFileReader randomAccessFileReader4 = fileStreamManager.getTsFileStreamReader(queryJob1, PATH);
        try {
            randomAccessFileReader3.read();
            fileStreamManager.releaseAll(queryJob1);
            randomAccessFileReader4.read();
            Assert.fail();
        } catch (IOException e) {
            Assert.assertEquals("Stream Closed", e.getMessage());
        }
    }

    @Test
    public void testMultiThread() throws InterruptedException {
        Reader reader1 = new Reader(new SelectQueryJob(1L));
        Reader reader2 = new Reader(new SelectQueryJob(2L));
        Reader reader3 = new Reader(new SelectQueryJob(3L));
        reader1.start();
        reader2.start();
        reader3.start();
        reader1.join();
        reader2.join();
        reader3.join();
        Assert.assertEquals(true, reader1.result);
        Assert.assertEquals(true, reader2.result);
        Assert.assertEquals(true, reader3.result);
    }

    private class Reader extends Thread {

        QueryJob queryJob;
        boolean result;

        public Reader(QueryJob queryJob) {
            super();
            this.queryJob = queryJob;
            this.result = false;
        }

        public void run() {
            try {
                ITsRandomAccessFileReader randomAccessFileReader = fileStreamManager.getTsFileStreamReader(queryJob, PATH);
                int v = randomAccessFileReader.read();
                Assert.assertEquals(1, v);
                fileStreamManager.releaseAll(queryJob);
                randomAccessFileReader.read();
                Assert.fail();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                Assert.assertEquals("Stream Closed", e.getMessage());
                result = true;
            }
        }
    }
}
