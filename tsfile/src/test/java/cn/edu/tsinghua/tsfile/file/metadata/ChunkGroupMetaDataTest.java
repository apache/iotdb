package cn.edu.tsinghua.tsfile.file.metadata;

import java.io.*;

import cn.edu.tsinghua.tsfile.file.metadata.utils.TestHelper;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ChunkGroupMetaDataTest {

  public static final String DELTA_OBJECT_UID = "delta-3312";
  final String PATH = "target/outputChunkGroup.tsfile";

  @Before
  public void setUp() {}

  @After
  public void tearDown() {
    File file = new File(PATH);
    if (file.exists())
      file.delete();
  }

  @Test
  public void testWriteIntoFile()  {
    // serialize metadata to a file
    ChunkGroupMetaData metaData = TestHelper.createSimpleChunkGroupMetaData();
    serialized(metaData);
    ChunkGroupMetaData readMetaData = deSerialized();
    serialized(readMetaData);
  }

  private ChunkGroupMetaData deSerialized() {
        FileInputStream fis = null;
        ChunkGroupMetaData metaData = null;
        try {
            fis = new FileInputStream(new File(PATH));
            metaData = ChunkGroupMetaData.deserializeFrom(fis);
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


    private void serialized(ChunkGroupMetaData metaData){
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
