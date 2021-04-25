package org.apache.iotdb.db.metadata.metadisk;

import org.apache.iotdb.db.metadata.MTreeDiskBasedTest;
import org.apache.iotdb.db.utils.EnvironmentUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

public class MetadataDiskManagerTest {

  private static final int CACHE_SIZE = 10;
  private static final String BASE_PATH = MTreeDiskBasedTest.class.getResource("").getPath();
  private static final String METAFILE_FILEPATH = BASE_PATH + "metafile.bin";

  @Before
  public void setUp() {
    EnvironmentUtils.envSetUp();
    File file = new File(METAFILE_FILEPATH);
    if (file.exists()) {
      file.delete();
    }
  }

  @After
  public void tearDown() throws Exception {
    File file = new File(METAFILE_FILEPATH);
    if (file.exists()) {
      file.delete();
    }
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testEviction() throws Exception {
    MetadataDiskManager manager = new MetadataDiskManager(CACHE_SIZE,METAFILE_FILEPATH);
    Assert.assertEquals("root", manager.getRoot().getName());
  }
}
