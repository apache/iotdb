package org.apache.iotdb.tsfile.fileSystem;

import org.apache.iotdb.tsfile.constant.TestConstant;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.file.Path;

public class FSPathTest {

  @Test
  public void testParse1() {
    FSPath fsPath;

    String pathWithFS =
        FSType.LOCAL.name() + FSPath.FS_PATH_SEPARATOR + TestConstant.BASE_OUTPUT_PATH;
    fsPath = FSPath.parse(pathWithFS);
    Assert.assertEquals(FSType.LOCAL, fsPath.getFsType());
    Assert.assertEquals(TestConstant.BASE_OUTPUT_PATH, fsPath.getPath());
    Assert.assertEquals(pathWithFS, fsPath.getRawFSPath());

    String pathWithOutFS = TestConstant.BASE_OUTPUT_PATH;
    fsPath = FSPath.parse(pathWithOutFS);
    Assert.assertEquals(FSType.LOCAL, fsPath.getFsType());
    Assert.assertEquals(TestConstant.BASE_OUTPUT_PATH, fsPath.getPath());
    Assert.assertEquals(pathWithFS, fsPath.getRawFSPath());
  }

  @Test
  public void testParse2() {
    File file = new File(TestConstant.BASE_OUTPUT_PATH);
    FSPath fsPath = FSPath.parse(file);
    Assert.assertEquals(FSType.LOCAL, fsPath.getFsType());
    Assert.assertEquals(file.getAbsolutePath(), fsPath.getPath());
    Assert.assertEquals(
        FSType.LOCAL.name() + FSPath.FS_PATH_SEPARATOR + file.getAbsolutePath(),
        fsPath.getRawFSPath());
  }

  @Test
  public void testToPath() {
    String pathWithFS =
        FSType.LOCAL.name() + FSPath.FS_PATH_SEPARATOR + TestConstant.BASE_OUTPUT_PATH;
    FSPath fsPath = FSPath.parse(pathWithFS);
    Path expectedPath = new File(TestConstant.BASE_OUTPUT_PATH).toPath();
    Assert.assertEquals(expectedPath, fsPath.toPath());
  }

  @Test
  public void testGetFile() {
    String pathWithFS =
        FSType.LOCAL.name() + FSPath.FS_PATH_SEPARATOR + TestConstant.BASE_OUTPUT_PATH;
    FSPath fsPath = FSPath.parse(pathWithFS);
    File expectedFile = new File(TestConstant.BASE_OUTPUT_PATH);
    Assert.assertEquals(expectedFile, fsPath.getFile());
  }

  @Test
  public void testGetChildFile() {
    String pathWithFS =
        FSType.LOCAL.name() + FSPath.FS_PATH_SEPARATOR + TestConstant.BASE_OUTPUT_PATH;
    FSPath fsPath = FSPath.parse(pathWithFS);
    String childFileName = "test.tsfile";
    File childFile = fsPath.getChildFile(childFileName);
    File expectedFile = new File(TestConstant.BASE_OUTPUT_PATH, childFileName);
    Assert.assertEquals(expectedFile, childFile);
  }

  @Test
  public void testPreConcat() {
    String fileName = "test.tsfile";
    String[] prefix = {TestConstant.BASE_OUTPUT_PATH};
    String pathWithFS = FSType.LOCAL.name() + FSPath.FS_PATH_SEPARATOR + fileName;
    FSPath fsPath = FSPath.parse(pathWithFS).preConcat(prefix);
    Assert.assertEquals(FSType.LOCAL, fsPath.getFsType());
    Assert.assertEquals(TestConstant.BASE_OUTPUT_PATH + fileName, fsPath.getPath());
    Assert.assertEquals(
        FSType.LOCAL.name() + FSPath.FS_PATH_SEPARATOR + TestConstant.BASE_OUTPUT_PATH + fileName,
        fsPath.getRawFSPath());
  }

  @Test
  public void testPostConcat() {
    String fileName = "test.tsfile";
    String[] suffix = {fileName};
    String pathWithFS =
        FSType.LOCAL.name() + FSPath.FS_PATH_SEPARATOR + TestConstant.BASE_OUTPUT_PATH;
    FSPath fsPath = FSPath.parse(pathWithFS).postConcat(suffix);
    Assert.assertEquals(FSType.LOCAL, fsPath.getFsType());
    Assert.assertEquals(TestConstant.BASE_OUTPUT_PATH + fileName, fsPath.getPath());
    Assert.assertEquals(
        FSType.LOCAL.name() + FSPath.FS_PATH_SEPARATOR + TestConstant.BASE_OUTPUT_PATH + fileName,
        fsPath.getRawFSPath());
  }
}
