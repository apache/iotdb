package org.apache.iotdb.db.metadata.metadisk.cache;

import org.apache.iotdb.db.metadata.*;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collections;

public class CachePerformanceTest {

  private static Logger logger = LoggerFactory.getLogger(CachePerformanceTest.class);

  private static final int TIMESERIES_NUM = 500;
  private static final int DEVICE_NUM = 1000;
  private static final double CACHE_RATIO = 0.75;
  private static final int NODE_NUM =
      (int) ((TIMESERIES_NUM * (DEVICE_NUM) + DEVICE_NUM + 3) * CACHE_RATIO);

  private static final String BASE_PATH = CachePerformanceTest.class.getResource("").getPath();
  private static final String METAFILE_FILEPATH = BASE_PATH + "metafile.bin";
  private File metaFile = new File(METAFILE_FILEPATH);

  private static PartialPath[][] paths = new PartialPath[DEVICE_NUM][TIMESERIES_NUM];

  @BeforeClass
  public static void initPath() throws Exception {
    long startTime = System.currentTimeMillis(), endTime;
    paths = new PartialPath[DEVICE_NUM][TIMESERIES_NUM];
    for (int i = 0; i < DEVICE_NUM; i++) {
      for (int j = 0; j < TIMESERIES_NUM; j++) {
        paths[i][j] = new PartialPath("root.t1.v1.d" + i + ".s" + j);
      }
    }
    endTime = System.currentTimeMillis();
    System.out.println("Path creation time cost: " + (endTime - startTime) + "ms");
  }

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
    if (metaFile.exists()) {
      metaFile.delete();
    }
  }

  @After
  public void tearDown() throws Exception {
    if (metaFile.exists()) {
      metaFile.delete();
    }
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void singleTimeComparison() throws Exception {
    System.gc();
    IMTree mTreeMem = testMTreeMem();
    mTreeMem.clear();
    mTreeMem = null;
    System.gc();
    IMTree mTreeDisk = testMTreeDisk();
    mTreeDisk.clear();
    //    mTreeDisk=null;
    //    System.out.println(ObjectSizeCalculator.getObjectSize(paths));
    //    System.out.println(ObjectSizeCalculator.getObjectSize(mTreeMem));
    //    System.out.println(ObjectSizeCalculator.getObjectSize(mTreeDisk));
    paths = null;
    System.gc();
    //        while (true) {}
  }

  private IMTree testMTreeMem() throws Exception {
    IMTree mTreeMem = new MTree();
    System.out.println("MTreeMem TS creation time cost: " + generateMTree(mTreeMem) + "ms");
    System.out.println("MTreeMem TS access time cost: " + accessMTree(mTreeMem) + "ms");
    return mTreeMem;
  }

  private IMTree testMTreeDisk() throws Exception {
    IMTree mTreeDisk = new MTreeDiskBased(NODE_NUM, METAFILE_FILEPATH);
    System.out.println("MTreeDisk TS creation time cost: " + generateMTree(mTreeDisk) + "ms");
    System.out.println("MTreeDisk TS access time cost: " + accessMTree(mTreeDisk) + "ms");
    return mTreeDisk;
  }

  @Test
  public void averagePerformanceComparison() throws Exception {
    System.gc();
    long createCostTime = 0, readCostTime = 0;
    int times = 10;
    for (int i = 0; i < times; i++) {
      MTree mTreeMem = new MTree();
      createCostTime += generateMTree(mTreeMem);
      readCostTime += accessMTree(mTreeMem);
      mTreeMem.clear();
      mTreeMem = null;
      System.gc();
    }
    System.out.println(
        "MTreeMem RW performance: "
            + createCostTime / times
            + "ms, "
            + readCostTime / times
            + "ms");

    long startTime = System.currentTimeMillis();
    System.gc();
    System.out.println("GC time cost: " + (System.currentTimeMillis() - startTime) + "ms");

    createCostTime = 0;
    readCostTime = 0;
    for (int i = 0; i < times; i++) {
      MTreeDiskBased mTreeDisk = new MTreeDiskBased(NODE_NUM, METAFILE_FILEPATH);
      createCostTime += generateMTree(mTreeDisk);
      readCostTime += accessMTree(mTreeDisk);
      mTreeDisk.clear();
      metaFile.delete();
      mTreeDisk = null;
      System.gc();
    }
    System.out.println(
        "MTreeDisk RW performance: "
            + createCostTime / times
            + "ms, "
            + readCostTime / times
            + "ms");
  }

  private long generateMTree(IMTree mTree) throws Exception {
    long startTime, endTime;
    mTree.setStorageGroup(new PartialPath("root.t1.v1"));
    startTime = System.currentTimeMillis();
    for (int i = 0; i < DEVICE_NUM; i++) {
      for (int j = 0; j < TIMESERIES_NUM; j++) {
        mTree.unlockMNode(
            mTree.createTimeseries(
                paths[i][j],
                TSDataType.TEXT,
                TSEncoding.PLAIN,
                TSFileDescriptor.getInstance().getConfig().getCompressor(),
                Collections.emptyMap(),
                null));
      }
    }
    endTime = System.currentTimeMillis();
    return endTime - startTime;
  }

  private long accessMTree(IMTree mTree) throws Exception {
    long startTime = System.currentTimeMillis();
    int missNum = 0;
    //    for (int i = 0; i < DEVICE_NUM; i++) {
    //      for (int j = 0; j < TIMESERIES_NUM; j++) {
    //        if (!mTree.isPathExist(paths[i][j])) {
    //          missNum++;
    //        }
    //      }
    //    }
    for (int i = DEVICE_NUM - 1; i >= 0; i--) {
      for (int j = TIMESERIES_NUM - 1; j >= 0; j--) {
        if (!mTree.isPathExist(paths[i][j])) {
          missNum++;
        }
      }
    }
    long endTime = System.currentTimeMillis();
    return endTime - startTime;
  }
}
