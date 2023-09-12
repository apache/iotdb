package org.apache.iotdb.db.it.last;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBLastViewIT {

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private String[] sqlList =
      new String[] {
        "create timeseries root.sg.d1.s1 INT32;",
        "create timeseries root.sg.d2.s1 INT32;",
        "create view root.sg.d3.vs1 as root.sg.d1.s1;",
        "create view root.sg.d3.vs2 as select d1.s1 + d2.s1 from root.sg;",
        "insert into root.sg.d1(time, s1) values (1, 1);",
        "insert into root.sg.d1(time, s1) values (106048000000, 2);",
        "insert into root.sg.d2(time, s1) values (1, 100);",
        "insert into root.sg.d2(time, s1) values (106048000000, 102);",
      };
}
