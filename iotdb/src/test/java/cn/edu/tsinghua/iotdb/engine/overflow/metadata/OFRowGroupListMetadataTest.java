package cn.edu.tsinghua.iotdb.engine.overflow.metadata;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;

public class OFRowGroupListMetadataTest {


    private String path = "OFRowGroupListMetadataTest";

    @Before
    public void setUp() throws Exception {
        

    }

    @After
    public void tearDown() throws Exception {
        File file = new File(path);
        if(file.exists()){
            file.delete();
        }
    }

    @Test
    public void testOFRowGroupListMetadata() throws Exception {
        OFRowGroupListMetadata ofRowGroupListMetadata = OverflowTestHelper.createOFRowGroupListMetadata();
        serialize(ofRowGroupListMetadata);
        OFRowGroupListMetadata deOfRowGroupListMetadata = deSerialized();
        OverflowUtils.isOFRowGroupListMetadataEqual(ofRowGroupListMetadata,deOfRowGroupListMetadata);
    }


    private void serialize(OFRowGroupListMetadata ofRowGroupListMetadata) throws FileNotFoundException {
        FileOutputStream outputStream = new FileOutputStream(path);
            try {
                ofRowGroupListMetadata.serializeTo(outputStream);
            } catch (IOException e) {
                e.printStackTrace();
            }finally {
                if(outputStream!=null){
                    try {
                        outputStream.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
    }

    private OFRowGroupListMetadata deSerialized() throws FileNotFoundException {
        FileInputStream inputStream = new FileInputStream(path);
        try {
            OFRowGroupListMetadata ofRowGroupListMetadata = OFRowGroupListMetadata.deserializeFrom(inputStream);
            return ofRowGroupListMetadata;
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if(inputStream!=null){
                try {
                    inputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return null;
    }
}