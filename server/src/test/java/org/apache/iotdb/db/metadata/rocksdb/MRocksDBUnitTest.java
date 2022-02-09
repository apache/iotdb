package org.apache.iotdb.db.metadata.rocksdb;

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.utils.FileUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.metadata.rocksdb.MRocksDBManager.ROCKSDB_PATH;

public class MRocksDBUnitTest {
  private MRocksDBWriter mRocksDBWriter;

  @Before
  public void setUp() throws MetadataException {
    mRocksDBWriter = new MRocksDBWriter();
  }

  @Test
  public void testSetStorageGroup() throws IllegalPathException {
    List<PartialPath> storageGroups = new ArrayList<>();
    storageGroups.add(new PartialPath("root.sg1"));
    storageGroups.add(new PartialPath("root.inner.sg1"));
    storageGroups.add(new PartialPath("root.inner.sg2"));
    storageGroups.add(new PartialPath("root.inner1.inner2.inner3.sg"));
    storageGroups.add(new PartialPath("root.inner1.inner2.sg"));

    try {
      for (PartialPath sg : storageGroups) {
        mRocksDBWriter.setStorageGroup(sg);
      }
      mRocksDBWriter.printScanAllKeys();
    } catch (Exception e) {
      assert (false);
    }
  }

  @Test
  public void testCreateTimeSeries() throws MetadataException, IOException {
    PartialPath path = new PartialPath("root.tt.sg.dd.m1");
    mRocksDBWriter.createTimeseries(
        path, TSDataType.TEXT, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED, null, null);

    PartialPath path2 = new PartialPath("root.tt.sg.dd.m2");
    mRocksDBWriter.createTimeseries(
        path2, TSDataType.TEXT, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED, null, "ma");
    mRocksDBWriter.printScanAllKeys();
  }

  @After
  public void clean() {
    mRocksDBWriter.close();
    resetEnv();
  }

  public void resetEnv() {
    File rockdDbFile = new File(ROCKSDB_PATH);
    if (rockdDbFile.exists() && rockdDbFile.isDirectory()) {
      FileUtils.deleteDirectory(rockdDbFile);
    }
  }
}
