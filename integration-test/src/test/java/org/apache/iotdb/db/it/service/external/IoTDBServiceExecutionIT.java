package org.apache.iotdb.db.it.service.external;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class IoTDBServiceExecutionIT {
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

  private static final int SERVICE_PORT = 5555;

  private static final int CONNECT_TIMEOUT_MS = 2000;

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testServiceExecution() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          String.format(
              "create service test AS '%s' using uri '%s'", SERVICE_CLASS_NAME, SERVICE_JAR_PATH));
      statement.execute("start service test");
      URL url = new URL("http://localhost:" + SERVICE_PORT);
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod("GET");
      conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
      assertEquals(200, conn.getResponseCode());
      conn.disconnect();

      statement.execute("stop service test");
      assertThrows(
          IOException.class,
          () -> {
            HttpURLConnection conn2 = (HttpURLConnection) url.openConnection();
            try {
              conn2.setRequestMethod("GET");
              conn2.setConnectTimeout(CONNECT_TIMEOUT_MS);
              conn2.getResponseCode();
            } finally {
              conn2.disconnect();
            }
          });
      statement.execute("drop service test");
    }
  }
}
