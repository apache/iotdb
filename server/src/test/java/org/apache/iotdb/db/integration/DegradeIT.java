package org.apache.iotdb.db.integration;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.compaction.CompactionStrategy;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.timeindex.TimeIndexLevel;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.rescon.TsFileResourceManager;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;

import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class DegradeIT {
  private static Connection connection;
  private static final Logger logger = LoggerFactory.getLogger(IoTDBFilePathUtilsIT.class);
  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();
  private TsFileResourceManager tsFileResourceManager = TsFileResourceManager.getInstance();
  private double timeIndexMemoryProportion;
  private long allocateMemoryForRead;
  private TimeIndexLevel timeIndexLevel;

  private static String[] unSeqSQLs =
      new String[]{
        "insert into root.sg1.d1(time,s1) values(1, 1)",
        "insert into root.sg1.d1(time,s2) values(2, 2)",
        "flush",
        "insert into root.sg1.d1(time,s1) values(9, 9)",
        "insert into root.sg1.d1(time,s2) values(10, 10)",
        "flush",
        "insert into root.sg1.d1(time,s1) values(5, 5)",
        "insert into root.sg1.d1(time,s2) values(6, 6)",
        "flush",
        "insert into root.sg1.d2(time,s1) values(11, 11)",
        "insert into root.sg1.d2(time,s2) values(12, 12)",
        "flush",
        "insert into root.sg1.d1(time,s1) values(13, 13)",
        "insert into root.sg1.d1(time,s2) values(14, 14)",
        "flush",
        "insert into root.sg1.d2(time,s1) values(7, 7)",
        "insert into root.sg1.d2(time,s2) values(8, 8)",
        "flush",
        "insert into root.sg1.d2(time,s1) values(3, 3)",
        "insert into root.sg1.d2(time,s2) values(4, 4)",
        "flush",
        "insert into root.sg1.d2(time,s1) values(15, 15)",
        "insert into root.sg1.d2(time,s2) values(16, 16)",
        "flush"
      };
  private static String[] seqSQLs =
      new String[] {
          "insert into root.sg1.d1(time,s1) values(1, 1)",
          "insert into root.sg1.d1(time,s2) values(2, 2)",
          "flush",
          "insert into root.sg1.d2(time,s1) values(3, 3)",
          "insert into root.sg1.d2(time,s2) values(4, 4)",
          "flush",
          "insert into root.sg1.d1(time,s1) values(5, 5)",
          "insert into root.sg1.d1(time,s2) values(6, 6)",
          "flush",
          "insert into root.sg1.d2(time,s1) values(7, 7)",
          "insert into root.sg1.d2(time,s2) values(8, 8)",
          "flush",
          "insert into root.sg1.d1(time,s1) values(9, 9)",
          "insert into root.sg1.d1(time,s2) values(10, 10)",
          "flush",
          "insert into root.sg1.d2(time,s1) values(11, 11)",
          "insert into root.sg1.d2(time,s2) values(12, 12)",
          "flush",
          "insert into root.sg1.d1(time,s1) values(13, 13)",
          "insert into root.sg1.d1(time,s2) values(14, 14)",
          "flush",
          "insert into root.sg1.d2(time,s1) values(15, 15)",
          "insert into root.sg1.d2(time,s2) values(16, 16)",
          "flush"
      };



  @Before
  public void setUp() throws ClassNotFoundException {
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
    timeIndexMemoryProportion = CONFIG.getTimeIndexMemoryProportion();
    allocateMemoryForRead = CONFIG.getAllocateMemoryForRead();
    timeIndexLevel = CONFIG.getTimeIndexLevel();
    Class.forName(Config.JDBC_DRIVER_NAME);
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setTimeIndexMemoryProportion(timeIndexMemoryProportion);
    IoTDBDescriptor.getInstance().getConfig().setTimeIndexLevel(String.valueOf(timeIndexLevel));
  }

  @Test
  public void test1() throws SQLException {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      double setTimeIndexMemoryProportion = 4 * Math.pow(10, -6);
      IoTDBDescriptor.getInstance()
              .getConfig()
              .setCompactionStrategy(CompactionStrategy.NO_COMPACTION);
//      IoTDBDescriptor.getInstance()
//          .getConfig()
//          .setTimeIndexMemoryProportion(setTimeIndexMemoryProportion);
      tsFileResourceManager.setTimeIndexMemoryThreshold(setTimeIndexMemoryProportion);
      for (String sql : seqSQLs) {
          statement.execute(sql);
      }
      statement.close();
      List<TsFileResource> SeqResources =
          new ArrayList<>(
              StorageEngine.getInstance()
                  .getProcessor(new PartialPath("root.sg1"))
                  .getSequenceFileTreeSet());
      assertEquals(8, SeqResources.size());
      for (TsFileResource resource: SeqResources) {
//        System.out.println(resource.getStartTime("root.sg1.d1"));
        System.out.println(TimeIndexLevel.FILE_TIME_INDEX ==
                TimeIndexLevel.valueOf(resource.getTimeIndexType()));
      }

      assertEquals(
          TimeIndexLevel.FILE_TIME_INDEX,
          TimeIndexLevel.valueOf(SeqResources.get(0).getTimeIndexType()));
      assertEquals(
          TimeIndexLevel.FILE_TIME_INDEX,
          TimeIndexLevel.valueOf(SeqResources.get(1).getTimeIndexType()));
      assertEquals(
          TimeIndexLevel.FILE_TIME_INDEX,
          TimeIndexLevel.valueOf(SeqResources.get(2).getTimeIndexType()));
      assertEquals(
          TimeIndexLevel.FILE_TIME_INDEX,
          TimeIndexLevel.valueOf(SeqResources.get(3).getTimeIndexType()));
      assertEquals(
          TimeIndexLevel.FILE_TIME_INDEX,
          TimeIndexLevel.valueOf(SeqResources.get(4).getTimeIndexType()));
      assertEquals(
          TimeIndexLevel.DEVICE_TIME_INDEX,
          TimeIndexLevel.valueOf(SeqResources.get(5).getTimeIndexType()));
      assertEquals(
          TimeIndexLevel.DEVICE_TIME_INDEX,
          TimeIndexLevel.valueOf(SeqResources.get(6).getTimeIndexType()));
      assertEquals(
          TimeIndexLevel.DEVICE_TIME_INDEX,
          TimeIndexLevel.valueOf(SeqResources.get(7).getTimeIndexType()));
    } catch (StorageEngineException | IllegalPathException e) {
      Assert.fail();
    }
  }

  @Test
  public void test2() throws SQLException {
    try (Connection connection =
                 DriverManager.getConnection(
                         Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
         Statement statement = connection.createStatement()) {
      double setTimeIndexMemoryProportion = 4 * Math.pow(10, -6);
      IoTDBDescriptor.getInstance()
              .getConfig()
              .setCompactionStrategy(CompactionStrategy.NO_COMPACTION);
//      IoTDBDescriptor.getInstance()
//              .getConfig()
//              .setTimeIndexMemoryProportion(setTimeIndexMemoryProportion);
      tsFileResourceManager.setTimeIndexMemoryThreshold(setTimeIndexMemoryProportion);
      for (String sql : unSeqSQLs) {
          statement.execute(sql);
      }
      statement.close();
      List<TsFileResource> SeqResources =
              new ArrayList<>(
                      StorageEngine.getInstance()
                              .getProcessor(new PartialPath("root.sg1"))
                              .getSequenceFileTreeSet());
      assertEquals(5, SeqResources.size());
      for (TsFileResource resource: SeqResources) {
        System.out.println(resource.getStartTime("root.sg1.d1"));
        System.out.println(TimeIndexLevel.FILE_TIME_INDEX ==
                TimeIndexLevel.valueOf(resource.getTimeIndexType()));
      }
      List<TsFileResource> UnSeqResources =
              new ArrayList<>(
                      StorageEngine.getInstance()
                              .getProcessor(new PartialPath("root.sg1"))
                              .getUnSequenceFileList());
      assertEquals(3, UnSeqResources.size());
      for (TsFileResource resource: UnSeqResources) {
        System.out.println(resource.getStartTime("root.sg1.d1"));
        System.out.println(TimeIndexLevel.FILE_TIME_INDEX ==
                TimeIndexLevel.valueOf(resource.getTimeIndexType()));
      }
      assertEquals(
              TimeIndexLevel.FILE_TIME_INDEX,
              TimeIndexLevel.valueOf(SeqResources.get(0).getTimeIndexType()));
      assertEquals(
              TimeIndexLevel.FILE_TIME_INDEX,
              TimeIndexLevel.valueOf(SeqResources.get(1).getTimeIndexType()));
      assertEquals(
              TimeIndexLevel.FILE_TIME_INDEX,
              TimeIndexLevel.valueOf(UnSeqResources.get(0).getTimeIndexType()));
      assertEquals(
              TimeIndexLevel.DEVICE_TIME_INDEX,
              TimeIndexLevel.valueOf(SeqResources.get(2).getTimeIndexType()));
      assertEquals(
              TimeIndexLevel.DEVICE_TIME_INDEX,
              TimeIndexLevel.valueOf(SeqResources.get(3).getTimeIndexType()));
      assertEquals(
              TimeIndexLevel.FILE_TIME_INDEX,
              TimeIndexLevel.valueOf(UnSeqResources.get(1).getTimeIndexType()));
      assertEquals(
              TimeIndexLevel.FILE_TIME_INDEX,
              TimeIndexLevel.valueOf(UnSeqResources.get(2).getTimeIndexType()));
      assertEquals(
              TimeIndexLevel.DEVICE_TIME_INDEX,
              TimeIndexLevel.valueOf(SeqResources.get(4).getTimeIndexType()));
    } catch (StorageEngineException | IllegalPathException e) {
      Assert.fail();
    }
  }

  @Test
  public void test3() throws SQLException {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      double setTimeIndexMemoryProportion = 0.9 * Math.pow(10, -6);
//      IoTDBDescriptor.getInstance()
//          .getConfig()
//          .setTimeIndexMemoryProportion(setTimeIndexMemoryProportion);
      tsFileResourceManager.setTimeIndexMemoryThreshold(setTimeIndexMemoryProportion);
      System.out.println(
              "memory cost before "
                      + CONFIG.getAllocateMemoryForRead()
                      + " "
                      + CONFIG.getTimeIndexMemoryProportion());
      statement.execute("insert into root.sg1.wf01.wt01(timestamp, status) values (1000, true)");
      statement.execute("insert into root.sg1.wf01.wt01(timestamp, status) values (2000, true)");
      statement.execute("insert into root.sg1.wf01.wt01(timestamp, status) values (3000, true)");
      statement.execute("flush");
      statement.close();
      List<TsFileResource> resources =
          new ArrayList<>(
              StorageEngine.getInstance()
                  .getProcessor(new PartialPath("root.sg1"))
                  .getSequenceFileTreeSet());
      assertEquals(1, resources.size());
      double timeIndexMemoryThreshold =
          CONFIG.getTimeIndexMemoryProportion() * CONFIG.getAllocateMemoryForRead();
      for (TsFileResource resource : resources) {
        System.out.println(
            "memory cost "
                + timeIndexMemoryThreshold
                + " "
                + resource.calculateRamSize()
                + " "
                + CONFIG.getAllocateMemoryForRead()
                + " "
                + CONFIG.getTimeIndexMemoryProportion());
        assertEquals(
            TimeIndexLevel.FILE_TIME_INDEX, TimeIndexLevel.valueOf(resource.getTimeIndexType()));
      }
    } catch (StorageEngineException | IllegalPathException e) {
      Assert.fail();
    }
  }
}
