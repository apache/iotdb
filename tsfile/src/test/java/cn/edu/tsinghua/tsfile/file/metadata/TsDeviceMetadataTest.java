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

public class TsDeviceMetadataTest {

    public static final long START_TIME = 523372036854775806L;
    public static final long END_TIME = 523372036854775806L;
    final String PATH = "target/outputDevice.tsfile";

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
    public void testWriteIntoFile() throws IOException {
        TsDeviceMetadata metaData = TestHelper.createSimpleDeviceMetaData();
        File file = new File(PATH);
        if (file.exists())
            file.delete();
        FileOutputStream fos = new FileOutputStream(file);
        metaData.serializeTo(fos);
        fos.close();

        FileInputStream fis = new FileInputStream(new File(PATH));
        Utils.isTsDeviceMetadataEqual(metaData, TsDeviceMetadata.deserializeFrom(fis));
        fis.close();
    }
}
