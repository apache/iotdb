package org.apache.iotdb.calcite;

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.util.Sources;
import org.apache.calcite.util.Util;
import org.apache.iotdb.calcite.utils.EnvironmentUtils;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.jdbc.Config;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;

import static org.junit.Assert.fail;

public class IoTDBAdapterTest {

  public static final ImmutableMap<String, String> MODEL =
          ImmutableMap.of("model",
                  Sources.of(IoTDBAdapterTest.class.getResource("/model.json"))
                          .file().getAbsolutePath());
  public static final String MODEL_STRING =
          Sources.of(IoTDBAdapterTest.class.getResource("/model.json")).file().getAbsolutePath();
  private static IoTDB daemon;
  private static String[] sqls = new String[]{

          "SET STORAGE GROUP TO root.vehicle",
          "SET STORAGE GROUP TO root.other",

          "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
          "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=INT64, ENCODING=RLE",
          "CREATE TIMESERIES root.vehicle.d0.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
          "CREATE TIMESERIES root.vehicle.d0.s3 WITH DATATYPE=TEXT, ENCODING=PLAIN",
          "CREATE TIMESERIES root.vehicle.d0.s4 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",

          "CREATE TIMESERIES root.vehicle.d1.s0 WITH DATATYPE=INT32, ENCODING=RLE",

          "CREATE TIMESERIES root.other.d1.s0 WITH DATATYPE=FLOAT, ENCODING=RLE",

          "insert into root.vehicle.d0(timestamp,s0) values(1,101)",
          "insert into root.vehicle.d0(timestamp,s0) values(2,198)",
          "insert into root.vehicle.d0(timestamp,s0) values(100,99)",
          "insert into root.vehicle.d0(timestamp,s0) values(101,99)",
          "insert into root.vehicle.d0(timestamp,s0) values(102,80)",
          "insert into root.vehicle.d0(timestamp,s0) values(103,99)",
          "insert into root.vehicle.d0(timestamp,s0) values(104,90)",
          "insert into root.vehicle.d0(timestamp,s0) values(105,99)",
          "insert into root.vehicle.d0(timestamp,s0) values(106,99)",
          "insert into root.vehicle.d0(timestamp,s0) values(2,10000)",
          "insert into root.vehicle.d0(timestamp,s0) values(50,10000)",
          "insert into root.vehicle.d0(timestamp,s0) values(1000,22222)",

          "insert into root.vehicle.d0(timestamp,s1) values(1,1101)",
          "insert into root.vehicle.d0(timestamp,s1) values(2,198)",
          "insert into root.vehicle.d0(timestamp,s1) values(100,199)",
          "insert into root.vehicle.d0(timestamp,s1) values(101,199)",
          "insert into root.vehicle.d0(timestamp,s1) values(102,180)",
          "insert into root.vehicle.d0(timestamp,s1) values(103,199)",
          "insert into root.vehicle.d0(timestamp,s1) values(104,190)",
          "insert into root.vehicle.d0(timestamp,s1) values(105,199)",
          "insert into root.vehicle.d0(timestamp,s1) values(2,40000)",
          "insert into root.vehicle.d0(timestamp,s1) values(50,50000)",
          "insert into root.vehicle.d0(timestamp,s1) values(1000,55555)",

          "insert into root.vehicle.d0(timestamp,s2) values(1000,55555)",
          "insert into root.vehicle.d0(timestamp,s2) values(2,2.22)",
          "insert into root.vehicle.d0(timestamp,s2) values(3,3.33)",
          "insert into root.vehicle.d0(timestamp,s2) values(4,4.44)",
          "insert into root.vehicle.d0(timestamp,s2) values(102,10.00)",
          "insert into root.vehicle.d0(timestamp,s2) values(105,11.11)",
          "insert into root.vehicle.d0(timestamp,s2) values(1000,1000.11)",

          "insert into root.vehicle.d0(timestamp,s3) values(60,'aaaaa')",
          "insert into root.vehicle.d0(timestamp,s3) values(70,'bbbbb')",
          "insert into root.vehicle.d0(timestamp,s3) values(80,'ccccc')",
          "insert into root.vehicle.d0(timestamp,s3) values(101,'ddddd')",
          "insert into root.vehicle.d0(timestamp,s3) values(102,'fffff')",

          "insert into root.vehicle.d1(timestamp,s0) values(1,999)",
          "insert into root.vehicle.d1(timestamp,s0) values(1000,888)",

          "insert into root.vehicle.d0(timestamp,s1) values(2000-01-01T08:00:00+08:00, 100)",
          "insert into root.vehicle.d0(timestamp,s3) values(2000-01-01T08:00:00+08:00, 'good')",

          "insert into root.vehicle.d0(timestamp,s4) values(100, false)",
          "insert into root.vehicle.d0(timestamp,s4) values(100, true)",

          "insert into root.other.d1(timestamp,s0) values(2, 3.14)",};

  @BeforeClass
  public static void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    daemon = IoTDB.getInstance();
    daemon.active();
    EnvironmentUtils.envSetUp();

    insertData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    daemon.stop();
    EnvironmentUtils.cleanEnv();
  }

  private static void insertData() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection = DriverManager
            .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
         Statement statement = connection.createStatement()) {

      for (String sql : sqls) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testSelect(){
    CalciteAssert.that()
            .with(MODEL)
            .query("select * from \"root.vehicle\"")
            .returnsCount(19)
            .returnsStartingWith("Time=1; Device=root.vehicle.d0; s0=101; s1=1101; s2=null; s3=null; s4=null")
            .explainContains("PLAN=IoTDBToEnumerableConverter\n" +
                    "  IoTDBTableScan(table=[[IoTDBSchema, root.vehicle]])");
  }

  /**
   * Test project without Time and Device columns
   */
  @Test
  public void testProject1() {
    CalciteAssert.that()
            .with(MODEL)
            .query("select \"s0\", \"s2\" from \"root.vehicle\"")
            .returnsStartingWith("s0=101; s2=null")
            .explainContains("PLAN=IoTDBToEnumerableConverter\n" +
                    "  IoTDBProject(s0=[$2], s2=[$4])\n" +
                    "    IoTDBTableScan(table=[[IoTDBSchema, root.vehicle]])");
  }

  /**
   * Test project with Time and Device columns
   */
  @Test
  public void testProject2() {
    CalciteAssert.that()
            .with(MODEL)
            .query("select \"Time\", \"Device\", \"s2\" from \"root.vehicle\"")
            .returnsStartingWith("Time=2; Device=root.vehicle.d0; s2=2.22")
            .explainContains("PLAN=IoTDBToEnumerableConverter\n" +
                    "  IoTDBProject(Time=[$0], Device=[$1], s2=[$4])\n" +
                    "    IoTDBTableScan(table=[[IoTDBSchema, root.vehicle]])");
  }

    @Test
    public void testProjectAlias() {
      CalciteAssert.that()
              .with(MODEL)
              .query("select \"Time\" AS \"t\", \"Device\" AS \"d\", \"s2\" from \"root.vehicle\"")
              .returnsStartingWith("t=2; d=root.vehicle.d0; s2=2.22")
              .explainContains("PLAN=IoTDBToEnumerableConverter\n" +
                      "  IoTDBProject(t=[$0], d=[$1], s2=[$4])\n" +
                      "    IoTDBTableScan(table=[[IoTDBSchema, root.vehicle]])");
  }

  @Test public void testLimitOffset() {
    CalciteAssert.that()
            .with(MODEL)
            .query("select \"Time\", \"s2\" from \"root.vehicle\" limit 3 offset 2")
            .explainContains("IoTDBLimit(limit=[3], offset=[2])\n")
            .returns("Time=3; s2=3.33\n" +
                     "Time=4; s2=4.44\n" +
                     "Time=50; s2=null\n");
  }

}
