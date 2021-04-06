package org.apache.iotdb.db.metadata;

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

import java.util.Collections;

public class MTreePerformanceTest {

  private static Logger logger = LoggerFactory.getLogger(MTreePerformanceTest.class);

  private static final int TIMESERIES_NUM = 10000;
  private static final int DEVICE_NUM = 100;
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
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void singleTimeComparison() throws Exception {
    System.gc();
    MTreeInterface mTreeMem = testMTreeMem();
    mTreeMem = null;
    System.gc();
    MTreeInterface mTreeDisk = testMTreeDisk();

    //        System.out.println(ObjectSizeCalculator.getObjectSize(paths));
    //        System.out.println(ObjectSizeCalculator.getObjectSize(mTreeMem));
    //        System.out.println(ObjectSizeCalculator.getObjectSize(mTreeDisk));

    //        while (true){
    //
    //        }
  }

  private MTreeInterface testMTreeMem() throws Exception {
    MTreeInterface mTreeMem = new MTree();
    System.out.println("MTreeMem TS creation time cost: " + generateMTree(mTreeMem) + "ms");
    System.out.println("MTreeMem TS access time cost: " + accessMTree(mTreeMem) + "ms");
    return mTreeMem;
  }

  private MTreeInterface testMTreeDisk() throws Exception {
    MTreeInterface mTreeDisk = new MTreeDiskBased();
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
      mTreeMem = null;
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
      MTreeDiskBased mTreeDisk = new MTreeDiskBased();
      createCostTime += generateMTree(mTreeDisk);
      readCostTime += accessMTree(mTreeDisk);
      mTreeDisk = null;
    }
    System.out.println(
        "MTreeDisk RW performance: "
            + createCostTime / times
            + "ms, "
            + readCostTime / times
            + "ms");
  }

  private long generateMTree(MTreeInterface mTree) throws Exception {
    long startTime, endTime;
    mTree.setStorageGroup(new PartialPath("root.t1.v1"));
    startTime = System.currentTimeMillis();
    for (int i = 0; i < DEVICE_NUM; i++) {
      for (int j = 0; j < TIMESERIES_NUM; j++) {
        mTree.createTimeseries(
            paths[i][j],
            TSDataType.TEXT,
            TSEncoding.PLAIN,
            TSFileDescriptor.getInstance().getConfig().getCompressor(),
            Collections.emptyMap(),
            null);
      }
    }
    endTime = System.currentTimeMillis();
    return endTime - startTime;
  }

  private long accessMTree(MTreeInterface mTree) throws Exception {
    long startTime = System.currentTimeMillis();
    for (int i = 0; i < DEVICE_NUM; i++) {
      for (int j = 0; j < TIMESERIES_NUM; j++) {
        mTree.isPathExist(paths[i][j]);
      }
    }
    long endTime = System.currentTimeMillis();
    return endTime - startTime;
  }
}
