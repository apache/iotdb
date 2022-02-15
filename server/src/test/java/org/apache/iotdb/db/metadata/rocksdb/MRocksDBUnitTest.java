package org.apache.iotdb.db.metadata.rocksdb;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.utils.FileUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.metadata.rocksdb.RocksDBReadWriteHandler.ROCKSDB_PATH;

public class MRocksDBUnitTest {

  private MRocksDBManager mRocksDBManager;

  @Before
  public void setUp() throws MetadataException {
    File file = new File(ROCKSDB_PATH);
    if (!file.exists()) {
      file.mkdirs();
    }
    mRocksDBManager = new MRocksDBManager();
  }

  @Test
  public void testStorageGroupOps() throws MetadataException, IOException, InterruptedException {
    List<PartialPath> storageGroups = new ArrayList<>();
    storageGroups.add(new PartialPath("root.sg1"));
    storageGroups.add(new PartialPath("root.inner.sg1"));
    storageGroups.add(new PartialPath("root.inner.sg2"));
    storageGroups.add(new PartialPath("root.inner1.inner2.inner3.sg"));
    storageGroups.add(new PartialPath("root.inner1.inner2.sg"));

    for (PartialPath sg : storageGroups) {
      mRocksDBManager.setStorageGroup(sg);
    }

    for (PartialPath sg : storageGroups) {
      mRocksDBManager.setTTL(sg, 200 * 10000);
    }

    mRocksDBManager.printScanAllKeys();

    Assert.assertTrue(mRocksDBManager.isPathExist(new PartialPath("root.sg1")));
    Assert.assertTrue(mRocksDBManager.isPathExist(new PartialPath("root.inner1.inner2.inner3")));
    Assert.assertFalse(mRocksDBManager.isPathExist(new PartialPath("root.inner1.inner5")));
    try {
      Assert.assertFalse(mRocksDBManager.isPathExist(new PartialPath("root.inner1...")));
    } catch (MetadataException e) {
      assert true;
    }

    Thread t1 =
        new Thread(
            () -> {
              try {
                List<PartialPath> toDelete = new ArrayList<>();
                toDelete.add(new PartialPath("root.sg1"));
                mRocksDBManager.deleteStorageGroups(toDelete);
              } catch (Exception e) {
                Assert.fail(e.getMessage());
              }
            });

    Thread t2 =
        new Thread(
            () -> {
              try {
                PartialPath path = new PartialPath("root.sg1.dd.m1");
                mRocksDBManager.createTimeseries(
                    path,
                    TSDataType.TEXT,
                    TSEncoding.PLAIN,
                    CompressionType.UNCOMPRESSED,
                    null,
                    null);
              } catch (Exception e) {
                Assert.fail(e.getMessage());
              }
            });

    t2.start();
    Thread.sleep(10);
    t1.start();
    Thread.sleep(10);

    PartialPath path = new PartialPath("root.sg1.dd.m2");
    Assert.assertThrows(
        MetadataException.class,
        () -> {
          mRocksDBManager.createTimeseries(
              path, TSDataType.TEXT, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED, null, null);
        });

    t1.join();
    t2.join();

    mRocksDBManager.printScanAllKeys();
  }

  @Test
  public void testCreateTimeSeries() throws MetadataException, IOException {
    PartialPath path = new PartialPath("root.tt.sg.dd.m1");
    mRocksDBManager.createTimeseries(
        path, TSDataType.TEXT, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED, null, null);

    PartialPath path2 = new PartialPath("root.tt.sg.dd.m2");
    mRocksDBManager.createTimeseries(
        path2, TSDataType.TEXT, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED, null, "ma");
    mRocksDBManager.printScanAllKeys();
  }

  @Test
  public void testCreateAlignedTimeSeries() throws MetadataException, IOException {
    PartialPath prefixPath = new PartialPath("root.tt.sg.dd");
    List<String> measurements = new ArrayList<>();
    List<TSDataType> dataTypes = new ArrayList<>();
    List<TSEncoding> encodings = new ArrayList<>();
    List<CompressionType> compressions = new ArrayList<>();

    for (int i = 0; i < 6; i++) {
      measurements.add("mm" + i);
      dataTypes.add(TSDataType.INT32);
      encodings.add(TSEncoding.PLAIN);
      compressions.add(CompressionType.UNCOMPRESSED);
    }
    mRocksDBManager.createAlignedTimeSeries(
        prefixPath, measurements, dataTypes, encodings, compressions);

    try {
      PartialPath path = new PartialPath("root.tt.sg.dd.mn");
      mRocksDBManager.createTimeseries(
          path, TSDataType.TEXT, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED, null, null);
      assert false;
    } catch (MetadataException e) {
      assert true;
    }
    mRocksDBManager.printScanAllKeys();
  }

  @Test
  public void testNodeTypeCount() throws MetadataException, IOException {
    List<PartialPath> storageGroups = new ArrayList<>();
    storageGroups.add(new PartialPath("root.sg1"));
    storageGroups.add(new PartialPath("root.inner.sg1"));
    storageGroups.add(new PartialPath("root.inner.sg2"));
    storageGroups.add(new PartialPath("root.inner1.inner2.inner3.sg"));
    storageGroups.add(new PartialPath("root.inner1.inner2.sg"));

    for (PartialPath sg : storageGroups) {
      mRocksDBManager.setStorageGroup(sg);
    }

    PartialPath path = new PartialPath("root.tt.sg.dd.m1");
    mRocksDBManager.createTimeseries(
        path, TSDataType.TEXT, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED, null, null);

    PartialPath path2 = new PartialPath("root.tt.sg.dd.m2");
    mRocksDBManager.createTimeseries(
        path2, TSDataType.TEXT, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED, null, "ma");
    mRocksDBManager.printScanAllKeys();

    Assert.assertEquals(
        1,
        mRocksDBManager.getStorageGroupNum(new PartialPath("root.inner1.inner2.inner3.sg"), false));
    Assert.assertEquals(2, mRocksDBManager.getStorageGroupNum(new PartialPath("root.inner"), true));
    Assert.assertEquals(6, mRocksDBManager.getStorageGroupNum(new PartialPath("root"), true));

    Assert.assertEquals(
        1, mRocksDBManager.getAllTimeseriesCount(new PartialPath("root.tt.sg.dd.m1")));
    Assert.assertEquals(2, mRocksDBManager.getAllTimeseriesCount(new PartialPath("root"), true));
    // todo wildcard

  }

  @After
  public void clean() {
    mRocksDBManager.close();
    resetEnv();
  }

  public void resetEnv() {
    File rockdDbFile = new File(ROCKSDB_PATH);
    if (rockdDbFile.exists() && rockdDbFile.isDirectory()) {
      FileUtils.deleteDirectory(rockdDbFile);
    }
  }
}
