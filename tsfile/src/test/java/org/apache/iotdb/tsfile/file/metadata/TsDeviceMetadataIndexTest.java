package org.apache.iotdb.tsfile.file.metadata;

import org.apache.iotdb.tsfile.file.metadata.utils.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;


public class TsDeviceMetadataIndexTest {

    private TsDeviceMetadataIndex index;

    private long offset = 10;
    private int len = 10;
    private long startTime = 100;
    private long endTime = 200;

    private File file;
    private String path = "target/TsDeviceMetadataIndex.tsfile";


    @Before
    public void setUp() {
        index = new TsDeviceMetadataIndex();
        index.setOffset(offset);
        index.setLen(len);
        index.setStartTime(startTime);
        index.setEndTime(endTime);
        file = new File(path);
    }

    @After
    public void tearDown() {
        file.delete();
    }

    @Test
    public void testSerDeDeviceMetadataIndex() throws IOException {
        OutputStream outputStream = new FileOutputStream(file);
        try {
            index.serializeTo(outputStream);
            InputStream inputStream = new FileInputStream(file);
            try {
                TsDeviceMetadataIndex index2 = TsDeviceMetadataIndex.deserializeFrom(inputStream);
                Utils.isTsDeviceMetadataIndexEqual(index, index2);
            } finally {
                inputStream.close();
            }
        } finally {
            outputStream.close();
        }
    }
}