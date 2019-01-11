package cn.edu.tsinghua.tsfile.file.metadata;

import cn.edu.tsinghua.tsfile.file.metadata.utils.TestHelper;
import cn.edu.tsinghua.tsfile.file.metadata.utils.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class TsFileMetaDataTest {
    final String PATH = "target/output1.tsfile";
    public static final int VERSION = 123;
    public static final String CREATED_BY = "tsf";

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    File file = new File(PATH);
    if (file.exists())
      file.delete();
    }

    @Test
    public void testWriteFileMetaData() throws IOException {
        TsFileMetaData tsfMetaData = TestHelper.createSimpleFileMetaData();
        serialized(tsfMetaData);
        TsFileMetaData readMetaData = deSerialized();
        Utils.isFileMetaDataEqual(tsfMetaData, readMetaData);
        serialized(readMetaData);
    }

    private TsFileMetaData deSerialized() {
        FileInputStream fis = null;
        TsFileMetaData metaData = null;
        try {
            fis = new FileInputStream(new File(PATH));
            metaData = TsFileMetaData.deserializeFrom(fis);
            return metaData;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (fis != null) {
                try {
                    fis.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return metaData;
    }


    private void serialized(TsFileMetaData metaData){
        File file = new File(PATH);
        if (file.exists()){
            file.delete();
        }
        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(file);
            metaData.serializeTo(fos);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if(fos!=null){
                try {
                    fos.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
