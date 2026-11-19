package org.apache.iotdb.relational.it.udf;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class UDAFMKIdentifyIT {

  private static final String UDF_CLASS = "org.apache.iotdb.udf.UDAFMKIdentify";

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @AfterClass
  public static void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @After
  public void dropAll() {
    SQLFunctionUtils.dropAllUDF();
  }

  @Test
  public void testUDAFMKIdentifyUdf() {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root");
      statement.execute("USE root");
      statement.execute(
          "CREATE TABLE test ( Time TIMESTAMP TIME, PlateText TEXT FIELD, VehicleID TEXT FIELD, CameraID TEXT FIELD ) WITH (TTL=31536000000)");

      statement.execute("INSERT INTO test(Time, \"PlateText\", \"VehicleID\", \"CameraID\") values ('2025-01-01T01:56:19.000+08:00', 'B0D3O444s', '100', '78')");
      statement.execute("INSERT INTO test(Time, \"PlateText\", \"VehicleID\", \"CameraID\") values ('2025-01-01T01:56:39.000+08:00', 'B0D3O444s', '100', '78')");
      statement.execute("INSERT INTO test(Time, \"PlateText\", \"VehicleID\", \"CameraID\") values ('2025-01-01T03:30:59.000+08:00', 'B0134s00', '2', '150')");

      statement.execute(String.format("CREATE FUNCTION mkidentify AS '%s'", UDF_CLASS));

      try (ResultSet rs = statement.executeQuery("SHOW FUNCTIONS")) {
        boolean found = false;
        while (rs.next()) {
          StringBuilder sb = new StringBuilder();
          for (int i = 1; i <= rs.getMetaData().getColumnCount(); ++i) {
            sb.append(rs.getString(i)).append(",");
          }
          String row = sb.toString();
          if (row.contains("UDAF") && row.contains(UDF_CLASS)) {
            found = true;
            break;
          }
        }
        assertTrue("mkidentify UDAF should appear in SHOW FUNCTIONS", found);
      }

      try (ResultSet rs = statement.executeQuery(
          "SELECT mkidentify(Time, PlateText, VehicleID, CameraID, 2, 0.2, 1.0) FROM root.test")) {
        int columnCount = rs.getMetaData().getColumnCount();
        assertEquals("Expected single output column from mkidentify", 1, columnCount);

        boolean hasRow = false;
        while (rs.next()) {
          hasRow = true;
          Object value = rs.getObject(1);
          Assert.assertNotNull("mkidentify returned null for a row", value);
        }
        assertTrue("mkidentify should return at least one row", hasRow);
      }

      statement.execute("DROP FUNCTION mkidentify");

      statement.execute("DROP DATABASE root");

    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }
}
