package org.apache.iotdb.tsfile.utils;

import org.apache.iotdb.tsfile.constant.TestConstant;
import org.apache.iotdb.tsfile.fileSystem.FSPath;
import org.apache.iotdb.tsfile.fileSystem.FSType;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.file.Path;

public class FSUtilsTest {

  @Test
  public void testGetFSTypeUsingFile() {
    File file = new File(TestConstant.BASE_OUTPUT_PATH);
    FSType fsType = FSUtils.getFSType(file);
    Assert.assertEquals(FSType.LOCAL, fsType);
  }

  @Test
  public void testGetFSTypeUsingPath() {
    Path path = new File(TestConstant.BASE_OUTPUT_PATH).toPath();
    FSType fsType = FSUtils.getFSType(path);
    Assert.assertEquals(FSType.LOCAL, fsType);
  }

  @Test
  public void testGetFSTypeUsingRawFSPath() {
    String pathWithFS =
        FSType.LOCAL.name() + FSPath.FS_PATH_SEPARATOR + TestConstant.BASE_OUTPUT_PATH;
    FSType fsType = FSUtils.getFSType(pathWithFS);
    Assert.assertEquals(FSType.LOCAL, fsType);
  }
}
