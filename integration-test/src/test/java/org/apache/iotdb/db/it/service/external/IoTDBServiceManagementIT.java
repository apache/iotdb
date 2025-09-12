package org.apache.iotdb.db.it.service.external;

import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.commons.service.external.ServiceStatus;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.jdbc.IoTDBSQLException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBServiceManagementIT {
  private static final String SERVICE_PATH_PREFIX =
      System.getProperty("user.dir")
          + File.separator
          + "target"
          + File.separator
          + "test-classes"
          + File.separator;
  private static final String SERVICE_JAR_PATH =
      new File(SERVICE_PATH_PREFIX).toURI() + "service/service-example.jar";

  private static final String SERVICE_CLASS_NAME = "org.apache.iotdb.service.TestService";

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testServiceManagementNormally() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          String.format(
              "create service test AS '%s' using uri '%s'", SERVICE_CLASS_NAME, SERVICE_JAR_PATH));
      try (ResultSet resultSet = statement.executeQuery("SHOW SERVICES")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
          String serviceName = resultSet.getString(ColumnHeaderConstant.SERVICE_NAME);
          String className = resultSet.getString(ColumnHeaderConstant.CLASS_NAME);
          String status = resultSet.getString(ColumnHeaderConstant.SERVICE_STATUS);
          assertEquals("test", serviceName);
          assertEquals(SERVICE_CLASS_NAME, className);
          assertEquals(ServiceStatus.INACTIVE.toString(), status);
        }
        assertEquals(1, count);
      } catch (Exception e) {
        fail(e.getMessage());
      }

      statement.execute("start service test");
      try (ResultSet resultSet = statement.executeQuery("SHOW SERVICES")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
          String status = resultSet.getString(ColumnHeaderConstant.SERVICE_STATUS);
          assertEquals(ServiceStatus.ACTIVE.toString(), status);
        }
        assertEquals(1, count);
      } catch (Exception e) {
        fail(e.getMessage());
      }

      statement.execute("stop service test");
      try (ResultSet resultSet = statement.executeQuery("SHOW SERVICES")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
          String status = resultSet.getString(ColumnHeaderConstant.SERVICE_STATUS);
          assertEquals(ServiceStatus.INACTIVE.toString(), status);
        }
        assertEquals(1, count);
      } catch (Exception e) {
        fail(e.getMessage());
      }

      statement.execute("drop service test");
      try (ResultSet resultSet = statement.executeQuery("show services")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(0, count);
      } catch (Exception e) {
        fail(e.getMessage());
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testCreateServiceWithError() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      IoTDBSQLException exception =
          assertThrows(
              IoTDBSQLException.class,
              () -> {
                statement.execute("show service test");
              });
      assertTrue(exception.getMessage().contains("Service [test] not found"));

      exception =
          assertThrows(
              IoTDBSQLException.class,
              () -> {
                statement.execute("stop service test");
              });
      assertTrue(exception.getMessage().contains("Service [test] not found"));

      exception =
          assertThrows(
              IoTDBSQLException.class,
              () -> {
                statement.execute("drop service test");
              });
      assertTrue(exception.getMessage().contains("Service [test] not found"));
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }
}
