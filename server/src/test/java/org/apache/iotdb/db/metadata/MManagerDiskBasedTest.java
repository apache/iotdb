package org.apache.iotdb.db.metadata;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.metadata.MetadataConstant.MTREE_DISK_BASED;
import static org.junit.Assert.*;

public class MManagerDiskBasedTest {

  private static Logger logger = LoggerFactory.getLogger(MManagerDiskBasedTest.class);

  private static final int TIMESERIES_NUM = 1000;
  private static final int DEVICE_NUM = 10;

  private CompressionType compressionType;
  private MManager manager = IoTDB.metaManager;

  @Before
  public void setUp() {
    compressionType = TSFileDescriptor.getInstance().getConfig().getCompressor();
    EnvironmentUtils.envSetUp();
    manager.clear();
    manager.setMTreeType(MTREE_DISK_BASED);
    manager.init();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  /** ************* Basic Test ************************************* */
  @Test
  public void basicTest_testAddPathAndExist() throws IllegalPathException {

    assertTrue(manager.isPathExist(new PartialPath("root")));

    assertFalse(manager.isPathExist(new PartialPath("root.laptop")));

    try {
      manager.setStorageGroup(new PartialPath("root.laptop.d1"));
      manager.setStorageGroup(new PartialPath("root.1"));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    assertTrue(manager.isPathExist(new PartialPath("root.1")));

    try {
      manager.setStorageGroup(new PartialPath("root.laptop"));
    } catch (MetadataException e) {
      Assert.assertEquals(
          "some children of root.laptop have already been set to storage group", e.getMessage());
    }

    try {
      manager.createTimeseries(
          new PartialPath("root.laptop.d1.s0"),
          TSDataType.valueOf("INT32"),
          TSEncoding.valueOf("RLE"),
          compressionType,
          Collections.emptyMap());
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    assertTrue(manager.isPathExist(new PartialPath("root.laptop")));
    assertTrue(manager.isPathExist(new PartialPath("root.laptop.d1")));
    assertTrue(manager.isPathExist(new PartialPath("root.laptop.d1.s0")));
    assertFalse(manager.isPathExist(new PartialPath("root.laptop.d1.s1")));
    try {
      manager.createTimeseries(
          new PartialPath("root.laptop.d1.s1"),
          TSDataType.valueOf("INT32"),
          TSEncoding.valueOf("RLE"),
          compressionType,
          Collections.emptyMap());
      manager.createTimeseries(
          new PartialPath("root.laptop.d1.1_2"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.EMPTY_MAP);
      manager.createTimeseries(
          new PartialPath("root.laptop.d1.\"1.2.3\""),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.EMPTY_MAP);
      manager.createTimeseries(
          new PartialPath("root.1.2.3"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.EMPTY_MAP);

      assertTrue(manager.isPathExist(new PartialPath("root.laptop.d1.s1")));
      assertTrue(manager.isPathExist(new PartialPath("root.laptop.d1.1_2")));
      assertTrue(manager.isPathExist(new PartialPath("root.laptop.d1.\"1.2.3\"")));
      assertTrue(manager.isPathExist(new PartialPath("root.1.2")));
      assertTrue(manager.isPathExist(new PartialPath("root.1.2.3")));
    } catch (MetadataException e1) {
      e1.printStackTrace();
      fail(e1.getMessage());
    }

    try {
      manager.deleteTimeseries(new PartialPath("root.laptop.d1.s1"));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    assertFalse(manager.isPathExist(new PartialPath("root.laptop.d1.s1")));

    try {
      manager.deleteTimeseries(new PartialPath("root.laptop.d1.s0"));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    assertFalse(manager.isPathExist(new PartialPath("root.laptop.d1.s0")));
    assertTrue(manager.isPathExist(new PartialPath("root.laptop.d1")));
    assertTrue(manager.isPathExist(new PartialPath("root.laptop")));
    assertTrue(manager.isPathExist(new PartialPath("root")));

    try {
      manager.createTimeseries(
          new PartialPath("root.laptop.d1.s1"),
          TSDataType.valueOf("INT32"),
          TSEncoding.valueOf("RLE"),
          compressionType,
          Collections.emptyMap());
    } catch (MetadataException e1) {
      e1.printStackTrace();
      fail(e1.getMessage());
    }

    try {
      manager.createTimeseries(
          new PartialPath("root.laptop.d1.s0"),
          TSDataType.valueOf("INT32"),
          TSEncoding.valueOf("RLE"),
          compressionType,
          Collections.emptyMap());
    } catch (MetadataException e1) {
      e1.printStackTrace();
      fail(e1.getMessage());
    }

    assertFalse(manager.isPathExist(new PartialPath("root.laptop.d2")));
    assertFalse(manager.checkStorageGroupByPath(new PartialPath("root.laptop.d2")));

    try {
      manager.deleteTimeseries(new PartialPath("root.laptop.d1.s0"));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    try {
      manager.deleteTimeseries(new PartialPath("root.laptop.d1.s1"));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    try {
      manager.setStorageGroup(new PartialPath("root.laptop1"));
    } catch (MetadataException e) {
      Assert.assertEquals(
          String.format(
              "The seriesPath of %s already exist, it can't be set to the storage group",
              "root.laptop1"),
          e.getMessage());
    }

    try {
      manager.deleteTimeseries(new PartialPath("root.laptop.d1.1_2"));
      manager.deleteTimeseries(new PartialPath("root.laptop.d1.\"1.2.3\""));
      manager.deleteTimeseries(new PartialPath("root.1.2.3"));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    assertFalse(manager.isPathExist(new PartialPath("root.laptop.d1.1_2")));
    assertFalse(manager.isPathExist(new PartialPath("root.laptop.d1.\"1.2.3\"")));
    assertFalse(manager.isPathExist(new PartialPath("root.1.2.3")));
    assertFalse(manager.isPathExist(new PartialPath("root.1.2")));
    assertTrue(manager.isPathExist(new PartialPath("root.1")));

    try {
      manager.deleteStorageGroups(Collections.singletonList(new PartialPath("root.1")));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    assertFalse(manager.isPathExist(new PartialPath("root.1")));
  }

  @Test
  public void basicTest_testGetAllTimeseriesCount() {

    try {
      manager.setStorageGroup(new PartialPath("root.laptop"));
      manager.createTimeseries(
          new PartialPath("root.laptop.d1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      manager.createTimeseries(
          new PartialPath("root.laptop.d1.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      manager.createTimeseries(
          new PartialPath("root.laptop.d1.s1.t1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      manager.createTimeseries(
          new PartialPath("root.laptop.d1.s2"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      manager.createTimeseries(
          new PartialPath("root.laptop.d2.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      manager.createTimeseries(
          new PartialPath("root.laptop.d2.s2"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);

      assertEquals(manager.getAllTimeseriesCount(new PartialPath("root")), 6);
      assertEquals(manager.getAllTimeseriesCount(new PartialPath("root.laptop")), 6);
      assertEquals(manager.getAllTimeseriesCount(new PartialPath("root.laptop.*")), 6);
      assertEquals(manager.getAllTimeseriesCount(new PartialPath("root.laptop.*.*")), 5);
      assertEquals(manager.getAllTimeseriesCount(new PartialPath("root.laptop.*.*.t1")), 1);
      assertEquals(manager.getAllTimeseriesCount(new PartialPath("root.laptop.*.s1")), 3);
      assertEquals(manager.getAllTimeseriesCount(new PartialPath("root.laptop.d1")), 4);
      assertEquals(manager.getAllTimeseriesCount(new PartialPath("root.laptop.d1.*")), 3);
      assertEquals(manager.getAllTimeseriesCount(new PartialPath("root.laptop.d2.s1")), 1);
      assertEquals(manager.getAllTimeseriesCount(new PartialPath("root.laptop.d2")), 2);

      try {
        manager.getAllTimeseriesCount(new PartialPath("root.laptop.d3.s1"));
        fail("Expected exception");
      } catch (MetadataException e) {
        assertEquals("Path [root.laptop.d3.s1] does not exist", e.getMessage());
      }
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void basicTest_testSetStorageGroupAndExist() {

    try {
      assertFalse(manager.isStorageGroup(new PartialPath("root")));
      assertFalse(manager.isStorageGroup(new PartialPath("root1.laptop.d2")));

      manager.setStorageGroup(new PartialPath("root.laptop.d1"));
      assertTrue(manager.isStorageGroup(new PartialPath("root.laptop.d1")));
      assertFalse(manager.isStorageGroup(new PartialPath("root.laptop.d2")));
      assertFalse(manager.isStorageGroup(new PartialPath("root.laptop")));
      assertFalse(manager.isStorageGroup(new PartialPath("root.laptop.d1.s1")));

      manager.setStorageGroup(new PartialPath("root.laptop.d2"));
      assertTrue(manager.isStorageGroup(new PartialPath("root.laptop.d1")));
      assertTrue(manager.isStorageGroup(new PartialPath("root.laptop.d2")));
      assertFalse(manager.isStorageGroup(new PartialPath("root.laptop.d3")));
      assertFalse(manager.isStorageGroup(new PartialPath("root.laptop")));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void basicTest_testRecover() {
    try {
      manager.setStorageGroup(new PartialPath("root.laptop.d1"));
      manager.setStorageGroup(new PartialPath("root.laptop.d2"));
      manager.createTimeseries(
          new PartialPath("root.laptop.d1.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      manager.createTimeseries(
          new PartialPath("root.laptop.d2.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      assertTrue(manager.isStorageGroup(new PartialPath("root.laptop.d1")));
      assertTrue(manager.isStorageGroup(new PartialPath("root.laptop.d2")));
      assertFalse(manager.isStorageGroup(new PartialPath("root.laptop.d3")));
      assertFalse(manager.isStorageGroup(new PartialPath("root.laptop")));
      Set<String> devices =
          new TreeSet<String>() {
            {
              add("root.laptop.d1");
              add("root.laptop.d2");
            }
          };
      // prefix with *
      assertEquals(
          devices,
          manager.getDevices(new PartialPath("root.*")).stream()
              .map(PartialPath::getFullPath)
              .collect(Collectors.toSet()));

      manager.deleteStorageGroups(Collections.singletonList(new PartialPath("root.laptop.d2")));
      assertTrue(manager.isStorageGroup(new PartialPath("root.laptop.d1")));
      assertFalse(manager.isStorageGroup(new PartialPath("root.laptop.d2")));
      assertFalse(manager.isStorageGroup(new PartialPath("root.laptop.d3")));
      assertFalse(manager.isStorageGroup(new PartialPath("root.laptop")));
      devices.remove("root.laptop.d2");
      // prefix with *
      assertEquals(
          devices,
          manager.getDevices(new PartialPath("root.*")).stream()
              .map(PartialPath::getFullPath)
              .collect(Collectors.toSet()));

      manager.clear(); // current manager will release the metafile occupation
      MManager recoverManager = new MManager();
      recoverManager.init();

      assertTrue(recoverManager.isStorageGroup(new PartialPath("root.laptop.d1")));
      assertFalse(recoverManager.isStorageGroup(new PartialPath("root.laptop.d2")));
      assertFalse(recoverManager.isStorageGroup(new PartialPath("root.laptop.d3")));
      assertFalse(recoverManager.isStorageGroup(new PartialPath("root.laptop")));
      // prefix with *
      assertEquals(
          devices,
          recoverManager.getDevices(new PartialPath("root.*")).stream()
              .map(PartialPath::getFullPath)
              .collect(Collectors.toSet()));

      recoverManager.clear();
      manager.init(); // manager need be init for env clean
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void basicTest_testRecoverFromFile() {
    try {
      manager.setStorageGroup(new PartialPath("root.laptop.d1"));
      manager.setStorageGroup(new PartialPath("root.laptop.d2"));
      manager.createTimeseries(
          new PartialPath("root.laptop.d1.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      manager.createTimeseries(
          new PartialPath("root.laptop.d2.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      assertTrue(manager.isStorageGroup(new PartialPath("root.laptop.d1")));
      assertTrue(manager.isStorageGroup(new PartialPath("root.laptop.d2")));
      assertFalse(manager.isStorageGroup(new PartialPath("root.laptop.d3")));
      assertFalse(manager.isStorageGroup(new PartialPath("root.laptop")));
      Set<String> devices =
          new TreeSet<String>() {
            {
              add("root.laptop.d1");
              add("root.laptop.d2");
            }
          };
      // prefix with *
      assertEquals(
          devices,
          manager.getDevices(new PartialPath("root.*")).stream()
              .map(PartialPath::getFullPath)
              .collect(Collectors.toSet()));

      manager.deleteStorageGroups(Collections.singletonList(new PartialPath("root.laptop.d2")));
      assertTrue(manager.isStorageGroup(new PartialPath("root.laptop.d1")));
      assertFalse(manager.isStorageGroup(new PartialPath("root.laptop.d2")));
      assertFalse(manager.isStorageGroup(new PartialPath("root.laptop.d3")));
      assertFalse(manager.isStorageGroup(new PartialPath("root.laptop")));
      devices.remove("root.laptop.d2");
      // prefix with *
      assertEquals(
          devices,
          manager.getDevices(new PartialPath("root.*")).stream()
              .map(PartialPath::getFullPath)
              .collect(Collectors.toSet()));

      manager.createMTreeSnapshot();
      manager.clear(); // current manager will release the metafile occupation

      MManager recoverManager = new MManager();
      recoverManager.setMTreeType(MTREE_DISK_BASED);
      recoverManager.init();

      assertTrue(recoverManager.isStorageGroup(new PartialPath("root.laptop.d1")));
      assertFalse(recoverManager.isStorageGroup(new PartialPath("root.laptop.d2")));
      assertFalse(recoverManager.isStorageGroup(new PartialPath("root.laptop.d3")));
      assertFalse(recoverManager.isStorageGroup(new PartialPath("root.laptop")));
      // prefix with *
      assertEquals(
          devices,
          recoverManager.getDevices(new PartialPath("root.*")).stream()
              .map(PartialPath::getFullPath)
              .collect(Collectors.toSet()));

      recoverManager.clear();
      manager.init(); // manager need be init for env clean
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void basicTest_testGetAllFileNamesByPath() {
    try {
      manager.setStorageGroup(new PartialPath("root.laptop.d1"));
      manager.setStorageGroup(new PartialPath("root.laptop.d2"));
      manager.createTimeseries(
          new PartialPath("root.laptop.d1.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      manager.createTimeseries(
          new PartialPath("root.laptop.d2.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);

      List<String> list = new ArrayList<>();

      list.add("root.laptop.d1");
      assertEquals(list, manager.getStorageGroupByPath(new PartialPath("root.laptop.d1.s1")));
      assertEquals(list, manager.getStorageGroupByPath(new PartialPath("root.laptop.d1")));
      list.add("root.laptop.d2");
      assertEquals(list, manager.getStorageGroupByPath(new PartialPath("root.laptop")));
      assertEquals(list, manager.getStorageGroupByPath(new PartialPath("root")));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void basicTest_testCheckStorageExistOfPath() {
    try {
      assertTrue(manager.getAllTimeseriesPath(new PartialPath("root")).isEmpty());
      assertTrue(manager.getStorageGroupByPath(new PartialPath("root.vehicle")).isEmpty());
      assertTrue(manager.getStorageGroupByPath(new PartialPath("root.vehicle.device")).isEmpty());
      assertTrue(
          manager.getStorageGroupByPath(new PartialPath("root.vehicle.device.sensor")).isEmpty());

      manager.setStorageGroup(new PartialPath("root.vehicle"));
      assertFalse(manager.getStorageGroupByPath(new PartialPath("root.vehicle")).isEmpty());
      assertFalse(manager.getStorageGroupByPath(new PartialPath("root.vehicle.device")).isEmpty());
      assertFalse(
          manager.getStorageGroupByPath(new PartialPath("root.vehicle.device.sensor")).isEmpty());
      assertTrue(manager.getStorageGroupByPath(new PartialPath("root.vehicle1")).isEmpty());
      assertTrue(manager.getStorageGroupByPath(new PartialPath("root.vehicle1.device")).isEmpty());

      manager.setStorageGroup(new PartialPath("root.vehicle1.device"));
      assertTrue(manager.getStorageGroupByPath(new PartialPath("root.vehicle1.device1")).isEmpty());
      assertTrue(manager.getStorageGroupByPath(new PartialPath("root.vehicle1.device2")).isEmpty());
      assertTrue(manager.getStorageGroupByPath(new PartialPath("root.vehicle1.device3")).isEmpty());
      assertFalse(manager.getStorageGroupByPath(new PartialPath("root.vehicle1.device")).isEmpty());
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void basicTest_testShowChildNodesWithGivenPrefix() {
    try {
      manager.setStorageGroup(new PartialPath("root.laptop"));
      manager.createTimeseries(
          new PartialPath("root.laptop.d1.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      manager.createTimeseries(
          new PartialPath("root.laptop.d2.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      manager.createTimeseries(
          new PartialPath("root.laptop.d1.s2"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      Set<String> nodes = new HashSet<>(Arrays.asList("s1", "s2"));
      Set<String> nodes2 = new HashSet<>(Arrays.asList("laptop"));
      Set<String> nodes3 = new HashSet<>(Arrays.asList("d1", "d2"));
      Set<String> nexLevelNodes1 =
          manager.getChildNodeInNextLevel(new PartialPath("root.laptop.d1"));
      Set<String> nexLevelNodes2 = manager.getChildNodeInNextLevel(new PartialPath("root"));
      Set<String> nexLevelNodes3 = manager.getChildNodeInNextLevel(new PartialPath("root.laptop"));
      // usual condition
      assertEquals(nodes, nexLevelNodes1);
      assertEquals(nodes2, nexLevelNodes2);
      assertEquals(nodes3, nexLevelNodes3);

    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void basicTest_testGetStorageGroupNameByAutoLevel() {
    int level = IoTDBDescriptor.getInstance().getConfig().getDefaultStorageGroupLevel();

    try {
      assertEquals(
          "root.laptop",
          MetaUtils.getStorageGroupPathByLevel(new PartialPath("root.laptop.d1.s1"), level)
              .getFullPath());
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    boolean caughtException = false;
    try {
      MetaUtils.getStorageGroupPathByLevel(new PartialPath("root1.laptop.d1.s1"), level);
    } catch (MetadataException e) {
      caughtException = true;
      assertEquals("root1.laptop.d1.s1 is not a legal path", e.getMessage());
    }
    assertTrue(caughtException);

    caughtException = false;
    try {
      MetaUtils.getStorageGroupPathByLevel(new PartialPath("root"), level);
    } catch (MetadataException e) {
      caughtException = true;
      assertEquals("root is not a legal path", e.getMessage());
    }
    assertTrue(caughtException);
  }

  @Test
  public void basicTest_testSetStorageGroupWithIllegalName() {
    try {
      PartialPath path1 = new PartialPath("root.laptop\n");
      try {
        manager.setStorageGroup(path1);
        fail();
      } catch (MetadataException e) {
      }
    } catch (IllegalPathException e1) {
      fail();
    }
    try {
      PartialPath path2 = new PartialPath("root.laptop\t");
      try {
        manager.setStorageGroup(path2);
        fail();
      } catch (MetadataException e) {
      }
    } catch (IllegalPathException e1) {
      fail();
    }
  }

  @Test
  public void basicTest_testCreateTimeseriesWithIllegalName() {
    try {
      PartialPath path1 = new PartialPath("root.laptop.d1\n.s1");
      try {
        manager.createTimeseries(
            path1, TSDataType.INT32, TSEncoding.PLAIN, CompressionType.SNAPPY, null);
        fail();
      } catch (MetadataException e) {
      }
    } catch (IllegalPathException e1) {
      fail();
    }
    try {
      PartialPath path2 = new PartialPath("root.laptop.d1\t.s1");
      try {
        manager.createTimeseries(
            path2, TSDataType.INT32, TSEncoding.PLAIN, CompressionType.SNAPPY, null);
        fail();
      } catch (MetadataException e) {
      }
    } catch (IllegalPathException e1) {
      fail();
    }
  }

  @Test
  public void basicTest_testGetDevicesWithGivenPrefix() {
    try {
      manager.setStorageGroup(new PartialPath("root.laptop"));
      manager.createTimeseries(
          new PartialPath("root.laptop.d1.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      manager.createTimeseries(
          new PartialPath("root.laptop.d2.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      Set<String> devices = new TreeSet<>();
      devices.add("root.laptop.d1");
      devices.add("root.laptop.d2");
      // usual condition
      assertEquals(
          devices,
          manager.getDevices(new PartialPath("root.laptop")).stream()
              .map(PartialPath::getFullPath)
              .collect(Collectors.toSet()));
      manager.setStorageGroup(new PartialPath("root.vehicle"));
      manager.createTimeseries(
          new PartialPath("root.vehicle.d1.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      devices.add("root.vehicle.d1");
      // prefix with *
      assertEquals(
          devices,
          manager.getDevices(new PartialPath("root.*")).stream()
              .map(PartialPath::getFullPath)
              .collect(Collectors.toSet()));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void basicTest_testGetChildNodePathInNextLevel() {
    String[] res =
        new String[] {
          "[root.laptop, root.vehicle]",
          "[root.laptop.b1, root.laptop.b2]",
          "[root.laptop.b1.d1, root.laptop.b1.d2]",
          "[root.laptop.b1, root.laptop.b2, root.vehicle.b1, root.vehicle.b2]",
          "[root.laptop.b1.d1, root.laptop.b1.d2, root.vehicle.b1.d0, root.vehicle.b1.d2, root.vehicle.b1.d3]",
          "[root.laptop.b1.d1, root.laptop.b1.d2]",
          "[root.vehicle.b1.d0, root.vehicle.b1.d2, root.vehicle.b1.d3, root.vehicle.b2.d0]",
          "[root.laptop.b1.d1.s0, root.laptop.b1.d1.s1, root.laptop.b1.d2.s0, root.laptop.b2.d1.s1, root.laptop.b2.d1.s3, root.laptop.b2.d2.s2]",
          "[]"
        };

    try {
      manager.setStorageGroup(new PartialPath("root.laptop"));
      manager.setStorageGroup(new PartialPath("root.vehicle"));

      manager.createTimeseries(
          new PartialPath("root.laptop.b1.d1.s0"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      manager.createTimeseries(
          new PartialPath("root.laptop.b1.d1.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      manager.createTimeseries(
          new PartialPath("root.laptop.b1.d2.s0"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      manager.createTimeseries(
          new PartialPath("root.laptop.b2.d1.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      manager.createTimeseries(
          new PartialPath("root.laptop.b2.d1.s3"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      manager.createTimeseries(
          new PartialPath("root.laptop.b2.d2.s2"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      manager.createTimeseries(
          new PartialPath("root.vehicle.b1.d0.s0"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      manager.createTimeseries(
          new PartialPath("root.vehicle.b1.d2.s2"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      manager.createTimeseries(
          new PartialPath("root.vehicle.b1.d3.s3"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      manager.createTimeseries(
          new PartialPath("root.vehicle.b2.d0.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);

      assertEquals(res[0], manager.getChildNodePathInNextLevel(new PartialPath("root")).toString());
      assertEquals(
          res[1], manager.getChildNodePathInNextLevel(new PartialPath("root.laptop")).toString());
      assertEquals(
          res[2],
          manager.getChildNodePathInNextLevel(new PartialPath("root.laptop.b1")).toString());
      assertEquals(
          res[3], manager.getChildNodePathInNextLevel(new PartialPath("root.*")).toString());
      assertEquals(
          res[4], manager.getChildNodePathInNextLevel(new PartialPath("root.*.b1")).toString());
      assertEquals(
          res[5], manager.getChildNodePathInNextLevel(new PartialPath("root.l*.b1")).toString());
      assertEquals(
          res[6], manager.getChildNodePathInNextLevel(new PartialPath("root.v*.*")).toString());
      assertEquals(
          res[7], manager.getChildNodePathInNextLevel(new PartialPath("root.l*.b*.*")).toString());
      assertEquals(
          res[8], manager.getChildNodePathInNextLevel(new PartialPath("root.laptopp")).toString());
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  /** ************* Advanced Test ************************************* */
  @Test
  public void advancedTest_test() {
    try {
      beforeAdvancedTest();
      // test file name
      List<PartialPath> fileNames = manager.getAllStorageGroupPaths();
      assertEquals(3, fileNames.size());
      if (fileNames.get(0).equals(new PartialPath("root.vehicle.d0"))) {
        assertEquals(new PartialPath("root.vehicle.d1"), fileNames.get(1));
      } else {
        assertEquals(new PartialPath("root.vehicle.d0"), fileNames.get(1));
      }
      // test filename by seriesPath
      assertEquals(
          new PartialPath("root.vehicle.d0"),
          manager.getStorageGroupPath(new PartialPath("root.vehicle.d0.s1")));
      List<PartialPath> pathList =
          manager.getAllTimeseriesPath(new PartialPath("root.vehicle.d1.*"));
      assertEquals(6, pathList.size());
      pathList = manager.getAllTimeseriesPath(new PartialPath("root.vehicle.d0"));
      assertEquals(6, pathList.size());
      pathList = manager.getAllTimeseriesPath(new PartialPath("root.vehicle.d*"));
      assertEquals(12, pathList.size());
      pathList = manager.getAllTimeseriesPath(new PartialPath("root.ve*.*"));
      assertEquals(12, pathList.size());
      pathList = manager.getAllTimeseriesPath(new PartialPath("root.vehicle*.d*.s1"));
      assertEquals(2, pathList.size());
      pathList = manager.getAllTimeseriesPath(new PartialPath("root.vehicle.d2"));
      assertEquals(0, pathList.size());
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void advancedTest_testCache() throws MetadataException {
    beforeAdvancedTest();
    manager.createTimeseries(
        new PartialPath("root.vehicle.d2.s0"),
        TSDataType.DOUBLE,
        TSEncoding.RLE,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap());
    manager.createTimeseries(
        new PartialPath("root.vehicle.d2.s1"),
        TSDataType.BOOLEAN,
        TSEncoding.PLAIN,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap());
    manager.createTimeseries(
        new PartialPath("root.vehicle.d2.s2.g0"),
        TSDataType.TEXT,
        TSEncoding.PLAIN,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap());
    manager.createTimeseries(
        new PartialPath("root.vehicle.d2.s3"),
        TSDataType.TEXT,
        TSEncoding.PLAIN,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap());

    IMNode node = manager.getNodeByPath(new PartialPath("root.vehicle.d0"));
    Assert.assertEquals(
        TSDataType.INT32,
        ((MeasurementMNode) manager.getNodeByPath(new PartialPath("root.vehicle.d0.s0")))
            .getSchema()
            .getType());

    try {
      manager.getNodeByPath(new PartialPath("root.vehicle.d100"));
    } catch (MetadataException e) {
      Assert.assertEquals("Path [root.vehicle.d100] does not exist", e.getMessage());
    }
  }

  @Test
  public void advancedTest_testCachedLastTimeValue() throws MetadataException {
    beforeAdvancedTest();
    manager.createTimeseries(
        new PartialPath("root.vehicle.d2.s0"),
        TSDataType.DOUBLE,
        TSEncoding.RLE,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap());

    TimeValuePair tv1 = new TimeValuePair(1000, TsPrimitiveType.getByType(TSDataType.DOUBLE, 1.0));
    TimeValuePair tv2 = new TimeValuePair(2000, TsPrimitiveType.getByType(TSDataType.DOUBLE, 3.0));
    TimeValuePair tv3 = new TimeValuePair(1500, TsPrimitiveType.getByType(TSDataType.DOUBLE, 2.5));
    IMNode node = manager.getNodeByPath(new PartialPath("root.vehicle.d2.s0"));
    ((MeasurementMNode) node).updateCachedLast(tv1, true, Long.MIN_VALUE);
    ((MeasurementMNode) node).updateCachedLast(tv2, true, Long.MIN_VALUE);
    Assert.assertEquals(
        tv2.getTimestamp(), ((MeasurementMNode) node).getCachedLast().getTimestamp());
    ((MeasurementMNode) node).updateCachedLast(tv3, true, Long.MIN_VALUE);
    Assert.assertEquals(
        tv2.getTimestamp(), ((MeasurementMNode) node).getCachedLast().getTimestamp());
  }

  private void beforeAdvancedTest() throws MetadataException {
    manager.setStorageGroup(new PartialPath("root.vehicle.d0"));
    manager.setStorageGroup(new PartialPath("root.vehicle.d1"));
    manager.setStorageGroup(new PartialPath("root.vehicle.d2"));

    manager.createTimeseries(
        new PartialPath("root.vehicle.d0.s0"),
        TSDataType.INT32,
        TSEncoding.RLE,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap());
    manager.createTimeseries(
        new PartialPath("root.vehicle.d0.s1"),
        TSDataType.INT64,
        TSEncoding.RLE,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap());
    manager.createTimeseries(
        new PartialPath("root.vehicle.d0.s2"),
        TSDataType.FLOAT,
        TSEncoding.RLE,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap());
    manager.createTimeseries(
        new PartialPath("root.vehicle.d0.s3"),
        TSDataType.DOUBLE,
        TSEncoding.RLE,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap());
    manager.createTimeseries(
        new PartialPath("root.vehicle.d0.s4"),
        TSDataType.BOOLEAN,
        TSEncoding.PLAIN,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap());
    manager.createTimeseries(
        new PartialPath("root.vehicle.d0.s5"),
        TSDataType.TEXT,
        TSEncoding.PLAIN,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap());

    manager.createTimeseries(
        new PartialPath("root.vehicle.d1.s0"),
        TSDataType.INT32,
        TSEncoding.RLE,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap());
    manager.createTimeseries(
        new PartialPath("root.vehicle.d1.s1"),
        TSDataType.INT64,
        TSEncoding.RLE,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap());
    manager.createTimeseries(
        new PartialPath("root.vehicle.d1.s2"),
        TSDataType.FLOAT,
        TSEncoding.RLE,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap());
    manager.createTimeseries(
        new PartialPath("root.vehicle.d1.s3"),
        TSDataType.DOUBLE,
        TSEncoding.RLE,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap());
    manager.createTimeseries(
        new PartialPath("root.vehicle.d1.s4"),
        TSDataType.BOOLEAN,
        TSEncoding.PLAIN,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap());
    manager.createTimeseries(
        new PartialPath("root.vehicle.d1.s5"),
        TSDataType.TEXT,
        TSEncoding.PLAIN,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap());
  }

  /** ************* Improve Test ************************************* */
  @Test
  public void improveTest_checkSetUp() throws MetadataException {
    beforeImproveTest();

    assertTrue(manager.isPathExist(new PartialPath("root.t1.v2.d3.s5")));
    assertFalse(manager.isPathExist(new PartialPath("root.t1.v2.d9.s" + TIMESERIES_NUM)));
    assertFalse(manager.isPathExist(new PartialPath("root.t10")));
  }

  @Test
  public void improveTest_analyseTimeCost() throws MetadataException {
    beforeImproveTest();

    long string_combine, path_exist, list_init, check_filelevel, get_seriestype;
    string_combine = path_exist = list_init = check_filelevel = get_seriestype = 0;

    String deviceId = "root.t1.v2.d3";
    String measurement = "s5";
    String path = deviceId + TsFileConstant.PATH_SEPARATOR + measurement;

    long startTime = System.currentTimeMillis();
    for (int i = 0; i < 100000; i++) {
      assertTrue(manager.isPathExist(new PartialPath(path)));
    }
    long endTime = System.currentTimeMillis();
    path_exist += endTime - startTime;

    startTime = System.currentTimeMillis();
    endTime = System.currentTimeMillis();
    list_init += endTime - startTime;

    startTime = System.currentTimeMillis();
    for (int i = 0; i < 100000; i++) {
      TSDataType dataType = manager.getSeriesType(new PartialPath(path));
      assertEquals(TSDataType.TEXT, dataType);
    }
    endTime = System.currentTimeMillis();
    get_seriestype += endTime - startTime;

    logger.debug("string combine:\t" + string_combine);
    logger.debug("seriesPath exist:\t" + path_exist);
    logger.debug("list init:\t" + list_init);
    logger.debug("check file level:\t" + check_filelevel);
    logger.debug("get series type:\t" + get_seriestype);
  }

  @Test
  public void improveTest_improveTest() throws IOException, MetadataException {
    beforeImproveTest();

    String[] deviceIdList = new String[DEVICE_NUM];
    for (int i = 0; i < DEVICE_NUM; i++) {
      deviceIdList[i] = "root.t1.v2.d" + i;
    }
    List<String> measurementList = new ArrayList<>();
    for (int i = 0; i < TIMESERIES_NUM; i++) {
      measurementList.add("s" + i);
    }

    long startTime = System.currentTimeMillis();
    for (String deviceId : deviceIdList) {
      doOriginTest(deviceId, measurementList);
    }
    long endTime = System.currentTimeMillis();
    logger.debug("origin:\t" + (endTime - startTime));

    startTime = System.currentTimeMillis();
    for (String deviceId : deviceIdList) {
      doPathLoopOnceTest(deviceId, measurementList);
    }
    endTime = System.currentTimeMillis();
    logger.debug("seriesPath loop once:\t" + (endTime - startTime));

    startTime = System.currentTimeMillis();
    for (String deviceId : deviceIdList) {
      doCacheTest(deviceId, measurementList);
    }
    endTime = System.currentTimeMillis();
    logger.debug("add cache:\t" + (endTime - startTime));
  }

  private void beforeImproveTest() throws MetadataException {
    manager.setStorageGroup(new PartialPath("root.t1.v2"));

    for (int j = 0; j < DEVICE_NUM; j++) {
      for (int i = 0; i < TIMESERIES_NUM; i++) {
        String p = "root.t1.v2.d" + j + ".s" + i;
        manager.createTimeseries(
            new PartialPath(p),
            TSDataType.TEXT,
            TSEncoding.PLAIN,
            TSFileDescriptor.getInstance().getConfig().getCompressor(),
            Collections.emptyMap());
      }
    }
  }

  private void doOriginTest(String deviceId, List<String> measurementList)
      throws MetadataException {
    for (String measurement : measurementList) {
      String path = deviceId + TsFileConstant.PATH_SEPARATOR + measurement;
      assertTrue(manager.isPathExist(new PartialPath(path)));
      TSDataType dataType = manager.getSeriesType(new PartialPath(path));
      assertEquals(TSDataType.TEXT, dataType);
    }
  }

  private void doPathLoopOnceTest(String deviceId, List<String> measurementList)
      throws MetadataException {
    for (String measurement : measurementList) {
      String path = deviceId + TsFileConstant.PATH_SEPARATOR + measurement;
      TSDataType dataType = manager.getSeriesType(new PartialPath(path));
      assertEquals(TSDataType.TEXT, dataType);
    }
  }

  private void doCacheTest(String deviceId, List<String> measurementList)
      throws MetadataException, IOException {
    IMNode node = manager.getDeviceNodeWithAutoCreate(new PartialPath(deviceId)).left;
    for (String s : measurementList) {
      assertTrue(node.hasChild(s));
      MeasurementMNode measurementNode =
          (MeasurementMNode) manager.getNodeByPath(new PartialPath(deviceId + "." + s));
      TSDataType dataType = measurementNode.getSchema().getType();
      assertEquals(TSDataType.TEXT, dataType);
    }
  }
}
