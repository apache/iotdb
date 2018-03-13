package cn.edu.tsinghua.iotdb.queryV2.control;

import cn.edu.tsinghua.iotdb.queryV2.engine.control.ConcurrentRandomAccessFile;
import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.timeseries.read.TsRandomAccessLocalFileReader;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhangjinrui on 2018/1/13.
 */
public class ConcurrentRandomAccessFileTest {

    private static final String PATH = "concurrentFile.io";

    @Before
    public void before() throws IOException {
        File file = new File(PATH);
        file.createNewFile();
    }

    @After
    public void after() {
        File file = new File(PATH);
        if (file.exists()) {
            file.delete();
        }
    }

    private byte[] writeFile(int size) throws IOException {
        byte[] bytes = new byte[size];
        for (int i = 0; i < size; i++) {
            bytes[i] = (byte) (i % 127 + 1);
        }
        FileOutputStream fileOutputStream = new FileOutputStream(PATH);
        fileOutputStream.write(bytes, 0, size);
        fileOutputStream.close();
        return bytes;
    }

    @Test
    public void testCorrectness() throws IOException, InterruptedException {
        byte[] bytes = writeFile(1000);
        ConcurrentRandomAccessFile concurrentRandomAcessFile = new ConcurrentRandomAccessFile(new RandomAccessFile(PATH, "r"));
        Reader reader1 = new Reader(concurrentRandomAcessFile, bytes);
        Reader reader2 = new Reader(concurrentRandomAcessFile, bytes);
        Reader reader3 = new Reader(concurrentRandomAcessFile, bytes);
        Thread thread1 = new Thread(reader1);
        Thread thread2 = new Thread(reader2);
        Thread thread3 = new Thread(reader3);
        thread1.start();
        Thread.sleep(100);
        thread2.start();
        thread3.start();
        thread1.join();
        thread2.join();
        thread3.join();
        concurrentRandomAcessFile.close();
        List<Byte> ret1 = reader1.getReadRet();
        List<Byte> ret2 = reader2.getReadRet();
        List<Byte> ret3 = reader3.getReadRet();
        for (int i = 0; i < bytes.length; i++) {
            Assert.assertEquals(bytes[i], (byte) ret1.get(i));
            Assert.assertEquals(bytes[i], (byte) ret2.get(i));
            Assert.assertEquals(bytes[i], (byte) ret3.get(i));
        }
    }

    private class Reader implements Runnable {
        private ConcurrentRandomAccessFile tsRandomAccessFileReader;
        private byte[] ret;
        private List<Byte> readRet;

        private Reader(ConcurrentRandomAccessFile tsRandomAccessFileReader, byte[] ret) {
            this.tsRandomAccessFileReader = tsRandomAccessFileReader;
            this.ret = ret;
            readRet = new ArrayList<>();
        }

        @Override
        public void run() {
            int index = 0;
            try {
                int b = 0;
                do {
                    b = tsRandomAccessFileReader.read();
                    if (b != -1) {
//                        System.out.println(Thread.currentThread().getName() + ":" + b);
                        Assert.assertEquals(ret[index], (byte) b);
                        readRet.add((byte) b);
                        index++;
                    }
                } while (b != -1);
                Assert.assertEquals(ret.length, readRet.size());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        public List<Byte> getReadRet() {
            return this.readRet;
        }
    }

    private enum readType {
        SINGLE_RANDOMACCESSFILE, CONCURRENT_RANDOMACCESSFILE;
    }

    @Test
    public void testEfficiency() throws IOException, InterruptedException {
        byte[] bytes = writeFile(100000);
        concurrentRead(readType.SINGLE_RANDOMACCESSFILE, 10, 100000);
    }

    private void concurrentRead(readType type, int threadCount, int blockSize) throws IOException, InterruptedException {
        ITsRandomAccessFileReader randomAccessFileReader = null;
        if (type == readType.CONCURRENT_RANDOMACCESSFILE) {
            randomAccessFileReader = new ConcurrentRandomAccessFile(new RandomAccessFile(PATH, "r"));
        }
        BlockReader blockReader[] = new BlockReader[threadCount];
        for (int i = 0; i < threadCount; i++) {
            if (type == readType.CONCURRENT_RANDOMACCESSFILE) {
                blockReader[i] = new BlockReader(randomAccessFileReader, blockSize);
            } else {
                blockReader[i] = new BlockReader(new TsRandomAccessLocalFileReader(PATH), blockSize);
            }
        }
        long startTimeStamp = System.currentTimeMillis();
        for (int i = 0; i < threadCount; i ++) {
            blockReader[i].start();
        }
        for (int i = 0; i < threadCount; i ++) {
            blockReader[i].join();
        }
        long endTimeStamp = System.currentTimeMillis();
        System.out.println(type + ". Time used for :" + (endTimeStamp - startTimeStamp) + "ms");
        if (type == readType.CONCURRENT_RANDOMACCESSFILE) {
            randomAccessFileReader.close();
        } else {
            for (int i = 0; i < threadCount; i ++) {
                blockReader[i].close();
            }
        }
    }

    private class BlockReader extends Thread {
        ITsRandomAccessFileReader tsRandomAccessFileReader;
        int blockSize;
        byte[] ret;

        public BlockReader(ITsRandomAccessFileReader tsRandomAccessFileReader, int blockSize) {
            super();
            this.tsRandomAccessFileReader = tsRandomAccessFileReader;
            this.blockSize = blockSize;
        }

        @Override
        public void run() {
            try {
                ret = new byte[(int) tsRandomAccessFileReader.length()];
                int offset = 0;
                while (offset < ret.length) {
                    int total = tsRandomAccessFileReader.read(ret, offset, blockSize);
                    offset += total;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        public byte[] getRet() {
            return ret;
        }

        public void close() throws IOException {
            this.tsRandomAccessFileReader.close();
        }
    }
}
