/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.relational.it.db.it;

import org.apache.iotdb.db.storageengine.dataregion.compaction.tablemodel.CompactionTableModelTestFileWriter;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.write.record.Tablet;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Locale;

import static org.apache.iotdb.db.it.utils.TestUtils.createUser;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBAuthenticationTableIT {
  @BeforeClass
  public static void setUpClass() throws Exception {
    Locale.setDefault(Locale.ENGLISH);
    EnvFactory.getEnv().getConfig().getCommonConfig().setEnforceStrongPassword(false);
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @After
  public void tearDown() throws Exception {
    try (ITableSession sessionRoot = EnvFactory.getEnv().getTableSessionConnection()) {
      String[] sqls =
          new String[] {
            "DROP USER userA",
            "DROP USER userB",
            "DROP USER userC",
            "DROP USER userD",
            "DROP ROLE role1",
            "DROP ROLE role2",
          };
      for (String sql : sqls) {
        try {
          sessionRoot.executeNonQueryStatement(sql);
        } catch (StatementExecutionException ignore) {
        }
      }
    }
  }

  @AfterClass
  public static void tearDownClass() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testInsert() throws IoTDBConnectionException, StatementExecutionException {

    try (ITableSession sessionRoot = EnvFactory.getEnv().getTableSessionConnection()) {

      try {
        sessionRoot.executeNonQueryStatement("CREATE DATABASE IF NOT EXISTS __audit");
        fail("Should have thrown an exception");
      } catch (StatementExecutionException e) {
        assertEquals("803: Access Denied: The database '__audit' is read-only.", e.getMessage());
      }

      sessionRoot.executeNonQueryStatement("CREATE DATABASE IF NOT EXISTS \"汉化\"");
      sessionRoot.executeNonQueryStatement("USE \"汉化\"");

      // insert by root
      Tablet tablet =
          new Tablet(
              "table1",
              Arrays.asList("id", "attr", "measurement"),
              Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.DOUBLE),
              Arrays.asList(ColumnCategory.TAG, ColumnCategory.ATTRIBUTE, ColumnCategory.FIELD));
      tablet.addTimestamp(0, 0);
      tablet.addValue(0, 0, "id1");
      tablet.addValue(0, 1, "attr1");
      tablet.addValue(0, 2, 0.1);

      sessionRoot.insert(tablet);

      sessionRoot.executeNonQueryStatement(
          "INSERT INTO table1 (time, id, attr, measurement) VALUES (1, 'id2', 'attr2', 0.2)");

      // revoke root
      try {
        sessionRoot.executeNonQueryStatement("REVOKE INSERT ON ANY FROM USER root");
        fail("Should have thrown an exception");
      } catch (StatementExecutionException e) {
        assertEquals(
            "803: Access Denied: Cannot grant/revoke privileges of admin user", e.getMessage());
      }
      try {
        sessionRoot.executeNonQueryStatement("REVOKE INSERT ON DATABASE \"汉化\" FROM USER root");
        fail("Should have thrown an exception");
      } catch (StatementExecutionException e) {
        assertEquals(
            "803: Access Denied: Cannot grant/revoke privileges of admin user", e.getMessage());
      }
      try {
        sessionRoot.executeNonQueryStatement("REVOKE INSERT ON TABLE table1 FROM USER root");
        fail("Should have thrown an exception");
      } catch (StatementExecutionException e) {
        assertEquals(
            "803: Access Denied: Cannot grant/revoke privileges of admin user", e.getMessage());
      }

      // test users
      sessionRoot.executeNonQueryStatement("CREATE USER userA 'userA1234567'");
      sessionRoot.executeNonQueryStatement("CREATE USER userB 'userB1234567'");
      // grant an irrelevant privilege so that the new users can use database
      sessionRoot.executeNonQueryStatement("GRANT SELECT ON DATABASE \"汉化\" TO USER userA");
      sessionRoot.executeNonQueryStatement("GRANT SELECT ON DATABASE \"汉化\" TO USER userB");

      try (ITableSession sessionA =
              EnvFactory.getEnv().getTableSessionConnection("userA", "userA1234567");
          ITableSession sessionB =
              EnvFactory.getEnv().getTableSessionConnection("userB", "userB1234567")) {
        sessionA.executeNonQueryStatement("USE \"汉化\"");
        sessionB.executeNonQueryStatement("USE \"汉化\"");
        // userA no privilege
        try {
          sessionA.executeNonQueryStatement(
              "INSERT INTO table1 (time, id, attr, measurement) VALUES (1, 'id2', 'attr2', 0.2)");
          fail("Should have thrown an exception");
        } catch (StatementExecutionException e) {
          assertEquals(
              "803: Access Denied: No permissions for this operation, please add privilege INSERT ON 汉化.table1",
              e.getMessage());
        }

        // grant and revoke - ALL
        sessionRoot.executeNonQueryStatement("GRANT ALL TO USER userA");
        sessionA.executeNonQueryStatement(
            "INSERT INTO table1 (time, id, attr, measurement) VALUES (1, 'id2', 'attr2', 0.2)");
        sessionRoot.executeNonQueryStatement("REVOKE ALL FROM USER userA");
        try {
          sessionA.executeNonQueryStatement(
              "INSERT INTO table1 (time, id, attr, measurement) VALUES (1, 'id2', 'attr2', 0.2)");
          fail("Should have thrown an exception");
        } catch (StatementExecutionException e) {
          assertEquals(
              "803: Access Denied: No permissions for this operation, please add privilege INSERT ON 汉化.table1",
              e.getMessage());
        }

        // grant and revoke - ANY
        sessionRoot.executeNonQueryStatement("GRANT INSERT ON ANY TO USER userA");
        sessionA.executeNonQueryStatement(
            "INSERT INTO table1 (time, id, attr, measurement) VALUES (1, 'id2', 'attr2', 0.2)");
        sessionRoot.executeNonQueryStatement("REVOKE INSERT ON ANY FROM USER userA");
        try {
          sessionA.executeNonQueryStatement(
              "INSERT INTO table1 (time, id, attr, measurement) VALUES (1, 'id2', 'attr2', 0.2)");
          fail("Should have thrown an exception");
        } catch (StatementExecutionException e) {
          assertEquals(
              "803: Access Denied: No permissions for this operation, please add privilege INSERT ON 汉化.table1",
              e.getMessage());
        }

        // grant and revoke - database
        sessionRoot.executeNonQueryStatement("GRANT INSERT ON DATABASE \"汉化\" TO USER userA");
        sessionA.executeNonQueryStatement(
            "INSERT INTO table1 (time, id, attr, measurement) VALUES (1, 'id2', 'attr2', 0.2)");
        sessionRoot.executeNonQueryStatement("REVOKE INSERT ON DATABASE \"汉化\" FROM USER userA");
        try {
          sessionA.executeNonQueryStatement(
              "INSERT INTO table1 (time, id, attr, measurement) VALUES (1, 'id2', 'attr2', 0.2)");
          fail("Should have thrown an exception");
        } catch (StatementExecutionException e) {
          assertEquals(
              "803: Access Denied: No permissions for this operation, please add privilege INSERT ON 汉化.table1",
              e.getMessage());
        }

        // grant and revoke - table
        sessionRoot.executeNonQueryStatement("GRANT INSERT ON TABLE table1 TO USER userA");
        sessionA.executeNonQueryStatement(
            "INSERT INTO table1 (time, id, attr, measurement) VALUES (1, 'id2', 'attr2', 0.2)");
        sessionRoot.executeNonQueryStatement("REVOKE INSERT ON TABLE table1 FROM USER userA");
        try {
          sessionA.executeNonQueryStatement(
              "INSERT INTO table1 (time, id, attr, measurement) VALUES (1, 'id2', 'attr2', 0.2)");
          fail("Should have thrown an exception");
        } catch (StatementExecutionException e) {
          assertEquals(
              "803: Access Denied: No permissions for this operation, please add privilege INSERT ON 汉化.table1",
              e.getMessage());
        }

        // can write but cannot auto-create
        sessionRoot.executeNonQueryStatement("GRANT INSERT ON DATABASE \"汉化\" TO USER userA");
        tablet.setTableName("table2");
        try {
          sessionA.insert(tablet);
          fail("Should have thrown an exception");
        } catch (StatementExecutionException e) {
          assertEquals(
              "803: Access Denied: No permissions for this operation, please add privilege CREATE ON 汉化.table2",
              e.getMessage());
        }
        sessionRoot.executeNonQueryStatement("GRANT CREATE ON DATABASE \"汉化\" TO USER userA");
        sessionA.insert(tablet);
        sessionRoot.executeNonQueryStatement("REVOKE CREATE ON DATABASE \"汉化\" FROM USER userA");
        sessionRoot.executeNonQueryStatement("REVOKE INSERT ON DATABASE \"汉化\" FROM USER userA");

        // can write but cannot add column
        sessionRoot.executeNonQueryStatement("GRANT INSERT ON DATABASE \"汉化\" TO USER userA");
        tablet =
            new Tablet(
                "table2",
                Arrays.asList("id", "attr", "measurement", "id2", "attr2", "measurement2"),
                Arrays.asList(
                    TSDataType.STRING,
                    TSDataType.STRING,
                    TSDataType.DOUBLE,
                    TSDataType.STRING,
                    TSDataType.STRING,
                    TSDataType.DOUBLE),
                Arrays.asList(
                    ColumnCategory.TAG,
                    ColumnCategory.ATTRIBUTE,
                    ColumnCategory.FIELD,
                    ColumnCategory.TAG,
                    ColumnCategory.ATTRIBUTE,
                    ColumnCategory.FIELD));
        tablet.addTimestamp(0, 0);
        tablet.addValue(0, 0, "id1");
        tablet.addValue(0, 1, "attr1");
        tablet.addValue(0, 2, 0.1);
        tablet.addValue(0, 3, "id2");
        tablet.addValue(0, 4, "attr2");
        tablet.addValue(0, 5, 0.2);
        try {
          sessionA.insert(tablet);
          fail("Should have thrown an exception");
        } catch (StatementExecutionException e) {
          assertEquals(
              "803: Access Denied: No permissions for this operation, please add privilege ALTER ON 汉化.table2",
              e.getMessage());
        }
        sessionRoot.executeNonQueryStatement("GRANT ALTER ON TABLE table2 TO USER userA");
        sessionA.insert(tablet);
        sessionRoot.executeNonQueryStatement("REVOKE ALTER ON TABLE table2 FROM USER userA");
        sessionRoot.executeNonQueryStatement("REVOKE INSERT ON DATABASE \"汉化\" FROM USER userA");

        // grant multiple and revoke one-by-one
        sessionRoot.executeNonQueryStatement("GRANT INSERT ON ANY TO USER userA");
        sessionRoot.executeNonQueryStatement("GRANT INSERT ON DATABASE \"汉化\" TO USER userA");
        sessionRoot.executeNonQueryStatement("GRANT INSERT ON TABLE table1 TO USER userA");
        sessionA.executeNonQueryStatement(
            "INSERT INTO table1 (time, id, attr, measurement) VALUES (1, 'id2', 'attr2', 0.2)");
        sessionRoot.executeNonQueryStatement("REVOKE INSERT ON ANY FROM USER userA");
        sessionA.executeNonQueryStatement(
            "INSERT INTO table1 (time, id, attr, measurement) VALUES (1, 'id2', 'attr2', 0.2)");
        sessionRoot.executeNonQueryStatement("REVOKE INSERT ON DATABASE \"汉化\" FROM USER userA");
        sessionA.executeNonQueryStatement(
            "INSERT INTO table1 (time, id, attr, measurement) VALUES (1, 'id2', 'attr2', 0.2)");
        sessionRoot.executeNonQueryStatement("REVOKE INSERT ON TABLE table1 FROM USER userA");
        try {
          sessionA.executeNonQueryStatement(
              "INSERT INTO table1 (time, id, attr, measurement) VALUES (1, 'id2', 'attr2', 0.2)");
          fail("Should have thrown an exception");
        } catch (StatementExecutionException e) {
          assertEquals(
              "803: Access Denied: No permissions for this operation, please add privilege INSERT ON 汉化.table1",
              e.getMessage());
        }

        // userA cannot revoke himself
        sessionRoot.executeNonQueryStatement("GRANT INSERT ON ANY TO USER userA");
        try {
          sessionA.executeNonQueryStatement("REVOKE INSERT ON ANY FROM USER userA");
          fail("Should have thrown an exception");
        } catch (StatementExecutionException e) {
          assertEquals(
              "803: Access Denied: No permissions for this operation, please add grant option to privilege INSERT",
              e.getMessage());
        }
        sessionRoot.executeNonQueryStatement("REVOKE INSERT ON ANY FROM USER userA");

        // userA can revoke himself
        sessionRoot.executeNonQueryStatement("GRANT INSERT ON ANY TO USER userA WITH GRANT OPTION");
        sessionA.executeNonQueryStatement("REVOKE INSERT ON ANY FROM USER userA");
        try {
          sessionA.executeNonQueryStatement(
              "INSERT INTO table1 (time, id, attr, measurement) VALUES (1, 'id2', 'attr2', 0.2)");
          fail("Should have thrown an exception");
        } catch (StatementExecutionException e) {
          assertEquals(
              "803: Access Denied: No permissions for this operation, please add privilege INSERT ON 汉化.table1",
              e.getMessage());
        }
        // after revoked cannot revoke again
        try {
          sessionA.executeNonQueryStatement("REVOKE INSERT ON ANY FROM USER userA");
          fail("Should have thrown an exception");
        } catch (StatementExecutionException e) {
          assertEquals(
              "803: Access Denied: No permissions for this operation, please add grant option to privilege INSERT",
              e.getMessage());
        }

        // userA cannot grant to userB
        try {
          sessionA.executeNonQueryStatement("GRANT INSERT ON ANY TO USER userB");
          fail("Should have thrown an exception");
        } catch (StatementExecutionException e) {
          assertEquals(
              "803: Access Denied: No permissions for this operation, please add grant option to privilege INSERT",
              e.getMessage());
        }
        sessionRoot.executeNonQueryStatement("GRANT INSERT ON ANY TO USER userA");
        try {
          sessionA.executeNonQueryStatement("GRANT INSERT ON ANY TO USER userB");
          fail("Should have thrown an exception");
        } catch (StatementExecutionException e) {
          assertEquals(
              "803: Access Denied: No permissions for this operation, please add grant option to privilege INSERT",
              e.getMessage());
        }
        try {
          sessionB.executeNonQueryStatement("REVOKE INSERT ON ANY FROM USER userA");
          fail("Should have thrown an exception");
        } catch (StatementExecutionException e) {
          assertEquals(
              "803: Access Denied: No permissions for this operation, please add grant option to privilege INSERT",
              e.getMessage());
        }

        // userA can grant to userB
        sessionRoot.executeNonQueryStatement("GRANT INSERT ON ANY TO USER userA WITH GRANT OPTION");
        sessionA.executeNonQueryStatement("GRANT INSERT ON ANY TO USER userB WITH GRANT OPTION");
        sessionB.executeNonQueryStatement(
            "INSERT INTO table1 (time, id, attr, measurement) VALUES (1, 'id2', 'attr2', 0.2)");
        // userB can revoke userA
        sessionB.executeNonQueryStatement("REVOKE INSERT ON ANY FROM USER userA");
        try {
          sessionA.executeNonQueryStatement(
              "INSERT INTO table1 (time, id, attr, measurement) VALUES (1, 'id2', 'attr2', 0.2)");
          fail("Should have thrown an exception");
        } catch (StatementExecutionException e) {
          assertEquals(
              "803: Access Denied: No permissions for this operation, please add privilege INSERT ON 汉化.table1",
              e.getMessage());
        }
        // userB can revoke himself
        sessionB.executeNonQueryStatement("REVOKE INSERT ON ANY FROM USER userB");
        try {
          sessionB.executeNonQueryStatement("REVOKE INSERT ON ANY FROM USER userA");
          fail("Should have thrown an exception");
        } catch (StatementExecutionException e) {
          assertEquals(
              "803: Access Denied: No permissions for this operation, please add grant option to privilege INSERT",
              e.getMessage());
        }
      }

      // test role
      sessionRoot.executeNonQueryStatement("CREATE USER userC 'userC1234567'");
      sessionRoot.executeNonQueryStatement("CREATE USER userD 'userD1234567'");
      sessionRoot.executeNonQueryStatement("CREATE ROLE role1");
      sessionRoot.executeNonQueryStatement("CREATE ROLE role2");
      sessionRoot.executeNonQueryStatement("GRANT ROLE role1 TO userC");
      sessionRoot.executeNonQueryStatement("GRANT ROLE role2 TO userD");

      try (ITableSession sessionC =
              EnvFactory.getEnv().getTableSessionConnection("userC", "userC1234567");
          ITableSession sessionD =
              EnvFactory.getEnv().getTableSessionConnection("userD", "userD1234567")) {
        // grant an irrelevant privilege so that the new users can use database
        sessionRoot.executeNonQueryStatement("GRANT SELECT ON DATABASE \"汉化\" TO USER userC");
        sessionRoot.executeNonQueryStatement("GRANT SELECT ON DATABASE \"汉化\" TO USER userD");
        sessionC.executeNonQueryStatement("USE \"汉化\"");
        sessionD.executeNonQueryStatement("USE \"汉化\"");
        // userC no privilege
        try {
          sessionC.executeNonQueryStatement(
              "INSERT INTO table1 (time, id, attr, measurement) VALUES (1, 'id2', 'attr2', 0.2)");
          fail("Should have thrown an exception");
        } catch (StatementExecutionException e) {
          assertEquals(
              "803: Access Denied: No permissions for this operation, please add privilege INSERT ON 汉化.table1",
              e.getMessage());
        }

        // grant and revoke - ALL
        sessionRoot.executeNonQueryStatement("GRANT ALL TO ROLE role1");
        sessionC.executeNonQueryStatement(
            "INSERT INTO table1 (time, id, attr, measurement) VALUES (1, 'id2', 'attr2', 0.2)");
        sessionRoot.executeNonQueryStatement("REVOKE ALL FROM ROLE role1");
        try {
          sessionC.executeNonQueryStatement(
              "INSERT INTO table1 (time, id, attr, measurement) VALUES (1, 'id2', 'attr2', 0.2)");
          fail("Should have thrown an exception");
        } catch (StatementExecutionException e) {
          assertEquals(
              "803: Access Denied: No permissions for this operation, please add privilege INSERT ON 汉化.table1",
              e.getMessage());
        }

        // grant and revoke - ANY
        sessionRoot.executeNonQueryStatement("GRANT INSERT ON ANY TO ROLE role1");
        sessionC.executeNonQueryStatement(
            "INSERT INTO table1 (time, id, attr, measurement) VALUES (1, 'id2', 'attr2', 0.2)");
        sessionRoot.executeNonQueryStatement("REVOKE INSERT ON ANY FROM ROLE role1");
        try {
          sessionC.executeNonQueryStatement(
              "INSERT INTO table1 (time, id, attr, measurement) VALUES (1, 'id2', 'attr2', 0.2)");
          fail("Should have thrown an exception");
        } catch (StatementExecutionException e) {
          assertEquals(
              "803: Access Denied: No permissions for this operation, please add privilege INSERT ON 汉化.table1",
              e.getMessage());
        }

        // grant and revoke - database
        sessionRoot.executeNonQueryStatement("GRANT INSERT ON DATABASE \"汉化\" TO ROLE role1");
        sessionC.executeNonQueryStatement(
            "INSERT INTO table1 (time, id, attr, measurement) VALUES (1, 'id2', 'attr2', 0.2)");
        sessionRoot.executeNonQueryStatement("REVOKE INSERT ON DATABASE \"汉化\" FROM ROLE role1");
        try {
          sessionC.executeNonQueryStatement(
              "INSERT INTO table1 (time, id, attr, measurement) VALUES (1, 'id2', 'attr2', 0.2)");
          fail("Should have thrown an exception");
        } catch (StatementExecutionException e) {
          assertEquals(
              "803: Access Denied: No permissions for this operation, please add privilege INSERT ON 汉化.table1",
              e.getMessage());
        }

        // grant and revoke - table
        sessionRoot.executeNonQueryStatement("GRANT INSERT ON TABLE table1 TO ROLE role1");
        sessionC.executeNonQueryStatement(
            "INSERT INTO table1 (time, id, attr, measurement) VALUES (1, 'id2', 'attr2', 0.2)");
        sessionRoot.executeNonQueryStatement("REVOKE INSERT ON TABLE table1 FROM ROLE role1");
        try {
          sessionC.executeNonQueryStatement(
              "INSERT INTO table1 (time, id, attr, measurement) VALUES (1, 'id2', 'attr2', 0.2)");
          fail("Should have thrown an exception");
        } catch (StatementExecutionException e) {
          assertEquals(
              "803: Access Denied: No permissions for this operation, please add privilege INSERT ON 汉化.table1",
              e.getMessage());
        }

        // grant multiple and revoke one-by-one
        sessionRoot.executeNonQueryStatement("GRANT INSERT ON ANY TO ROLE role1");
        sessionRoot.executeNonQueryStatement("GRANT INSERT ON DATABASE \"汉化\" TO ROLE role1");
        sessionRoot.executeNonQueryStatement("GRANT INSERT ON TABLE table1 TO ROLE role1");
        sessionC.executeNonQueryStatement(
            "INSERT INTO table1 (time, id, attr, measurement) VALUES (1, 'id2', 'attr2', 0.2)");
        sessionRoot.executeNonQueryStatement("REVOKE INSERT ON ANY FROM ROLE role1");
        sessionC.executeNonQueryStatement(
            "INSERT INTO table1 (time, id, attr, measurement) VALUES (1, 'id2', 'attr2', 0.2)");
        sessionRoot.executeNonQueryStatement("REVOKE INSERT ON DATABASE \"汉化\" FROM ROLE role1");
        sessionC.executeNonQueryStatement(
            "INSERT INTO table1 (time, id, attr, measurement) VALUES (1, 'id2', 'attr2', 0.2)");
        sessionRoot.executeNonQueryStatement("REVOKE INSERT ON TABLE table1 FROM ROLE role1");
        try {
          sessionC.executeNonQueryStatement(
              "INSERT INTO table1 (time, id, attr, measurement) VALUES (1, 'id2', 'attr2', 0.2)");
          fail("Should have thrown an exception");
        } catch (StatementExecutionException e) {
          assertEquals(
              "803: Access Denied: No permissions for this operation, please add privilege INSERT ON 汉化.table1",
              e.getMessage());
        }

        // role1 cannot revoke himself
        sessionRoot.executeNonQueryStatement("GRANT INSERT ON ANY TO ROLE role1");
        try {
          sessionC.executeNonQueryStatement("REVOKE INSERT ON ANY FROM ROLE role1");
          fail("Should have thrown an exception");
        } catch (StatementExecutionException e) {
          assertEquals(
              "803: Access Denied: No permissions for this operation, please add grant option to privilege INSERT",
              e.getMessage());
        }
        sessionRoot.executeNonQueryStatement("REVOKE INSERT ON ANY FROM ROLE role1");

        // role1 can revoke himself
        sessionRoot.executeNonQueryStatement("GRANT INSERT ON ANY TO ROLE role1 WITH GRANT OPTION");
        sessionC.executeNonQueryStatement("REVOKE INSERT ON ANY FROM ROLE role1");
        try {
          sessionC.executeNonQueryStatement(
              "INSERT INTO table1 (time, id, attr, measurement) VALUES (1, 'id2', 'attr2', 0.2)");
          fail("Should have thrown an exception");
        } catch (StatementExecutionException e) {
          assertEquals(
              "803: Access Denied: No permissions for this operation, please add privilege INSERT ON 汉化.table1",
              e.getMessage());
        }
        // after revoked cannot revoke again
        try {
          sessionC.executeNonQueryStatement("REVOKE INSERT ON ANY FROM ROLE role1");
          fail("Should have thrown an exception");
        } catch (StatementExecutionException e) {
          assertEquals(
              "803: Access Denied: No permissions for this operation, please add grant option to privilege INSERT",
              e.getMessage());
        }

        // role1 cannot grant to role2
        try {
          sessionC.executeNonQueryStatement("GRANT INSERT ON ANY TO ROLE role2");
          fail("Should have thrown an exception");
        } catch (StatementExecutionException e) {
          assertEquals(
              "803: Access Denied: No permissions for this operation, please add grant option to privilege INSERT",
              e.getMessage());
        }
        sessionRoot.executeNonQueryStatement("GRANT INSERT ON ANY TO ROLE role1");
        try {
          sessionC.executeNonQueryStatement("GRANT INSERT ON ANY TO ROLE role2");
          fail("Should have thrown an exception");
        } catch (StatementExecutionException e) {
          assertEquals(
              "803: Access Denied: No permissions for this operation, please add grant option to privilege INSERT",
              e.getMessage());
        }
        try {
          sessionD.executeNonQueryStatement(
              "INSERT INTO table1 (time, id, attr, measurement) VALUES (1, 'id2', 'attr2', 0.2)");
          fail("Should have thrown an exception");
        } catch (StatementExecutionException e) {
          assertEquals(
              "803: Access Denied: No permissions for this operation, please add privilege INSERT ON 汉化.table1",
              e.getMessage());
        }

        // userC can grant to userD
        sessionRoot.executeNonQueryStatement("GRANT INSERT ON ANY TO ROLE role1 WITH GRANT OPTION");
        sessionC.executeNonQueryStatement("GRANT INSERT ON ANY TO ROLE role2 WITH GRANT OPTION");
        sessionD.executeNonQueryStatement(
            "INSERT INTO table1 (time, id, attr, measurement) VALUES (1, 'id2', 'attr2', 0.2)");
        // userD can revoke userC
        sessionD.executeNonQueryStatement("REVOKE INSERT ON ANY FROM ROLE role1");
        try {
          sessionC.executeNonQueryStatement(
              "INSERT INTO table1 (time, id, attr, measurement) VALUES (1, 'id2', 'attr2', 0.2)");
          fail("Should have thrown an exception");
        } catch (StatementExecutionException e) {
          assertEquals(
              "803: Access Denied: No permissions for this operation, please add privilege INSERT ON 汉化.table1",
              e.getMessage());
        }
        // userD can revoke himself
        sessionD.executeNonQueryStatement("REVOKE INSERT ON ANY FROM ROLE role2");
        try {
          sessionD.executeNonQueryStatement("REVOKE INSERT ON ANY FROM ROLE role1");
          fail("Should have thrown an exception");
        } catch (StatementExecutionException e) {
          assertEquals(
              "803: Access Denied: No permissions for this operation, please add grant option to privilege INSERT",
              e.getMessage());
        }

        // lose privilege after role is revoked
        sessionRoot.executeNonQueryStatement("GRANT INSERT ON ANY TO ROLE role1");
        sessionRoot.executeNonQueryStatement("REVOKE ROLE role1 FROM userC");
        try {
          sessionC.executeNonQueryStatement(
              "INSERT INTO table1 (time, id, attr, measurement) VALUES (1, 'id2', 'attr2', 0.2)");
          fail("Should have thrown an exception");
        } catch (StatementExecutionException e) {
          assertEquals(
              "803: Access Denied: No permissions for this operation, please add privilege INSERT ON 汉化.table1",
              e.getMessage());
        }
      }
    }
  }

  @Test
  public void testDelete() throws IoTDBConnectionException, StatementExecutionException {

    try (ITableSession sessionRoot = EnvFactory.getEnv().getTableSessionConnection()) {
      sessionRoot.executeNonQueryStatement("CREATE DATABASE test2");
      sessionRoot.executeNonQueryStatement("USE test2");

      // insert by root
      Tablet tablet =
          new Tablet(
              "table1",
              Arrays.asList("id", "attr", "measurement"),
              Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.DOUBLE),
              Arrays.asList(ColumnCategory.TAG, ColumnCategory.ATTRIBUTE, ColumnCategory.FIELD));
      tablet.addTimestamp(0, 0);
      tablet.addValue(0, 0, "id1");
      tablet.addValue(0, 1, "attr1");
      tablet.addValue(0, 2, 0.1);

      sessionRoot.insert(tablet);

      sessionRoot.executeNonQueryStatement("DELETE FROM table1");

      // revoke root
      try {
        sessionRoot.executeNonQueryStatement("REVOKE DELETE ON ANY FROM USER root");
        fail("Should have thrown an exception");
      } catch (StatementExecutionException e) {
        assertEquals(
            "803: Access Denied: Cannot grant/revoke privileges of admin user", e.getMessage());
      }
      try {
        sessionRoot.executeNonQueryStatement("REVOKE DELETE ON DATABASE test FROM USER root");
        fail("Should have thrown an exception");
      } catch (StatementExecutionException e) {
        assertEquals(
            "803: Access Denied: Cannot grant/revoke privileges of admin user", e.getMessage());
      }
      try {
        sessionRoot.executeNonQueryStatement("REVOKE DELETE ON TABLE table1 FROM USER root");
        fail("Should have thrown an exception");
      } catch (StatementExecutionException e) {
        assertEquals(
            "803: Access Denied: Cannot grant/revoke privileges of admin user", e.getMessage());
      }

      // test users
      sessionRoot.executeNonQueryStatement("CREATE USER userA 'userA1234567'");
      sessionRoot.executeNonQueryStatement("CREATE USER userB 'userB1234567'");

      try (ITableSession sessionA =
              EnvFactory.getEnv().getTableSessionConnection("userA", "userA1234567");
          ITableSession sessionB =
              EnvFactory.getEnv().getTableSessionConnection("userB", "userB1234567")) {
        // grant an irrelevant privilege so that the new users can use database
        sessionRoot.executeNonQueryStatement("GRANT SELECT ON DATABASE test2 TO USER userA");
        sessionRoot.executeNonQueryStatement("GRANT SELECT ON DATABASE test2 TO USER userB");
        sessionA.executeNonQueryStatement("USE test2");
        sessionB.executeNonQueryStatement("USE test2");
        // userA no privilege
        try {
          sessionA.executeNonQueryStatement("DELETE FROM table1");
          fail("Should have thrown an exception");
        } catch (StatementExecutionException e) {
          assertEquals(
              "803: Access Denied: No permissions for this operation, please add privilege DELETE ON test2.table1",
              e.getMessage());
        }

        // grant and revoke - ALL
        sessionRoot.executeNonQueryStatement("GRANT ALL TO USER userA");
        sessionA.executeNonQueryStatement("DELETE FROM table1");
        sessionRoot.executeNonQueryStatement("REVOKE ALL FROM USER userA");
        try {
          sessionA.executeNonQueryStatement("DELETE FROM table1");
          fail("Should have thrown an exception");
        } catch (StatementExecutionException e) {
          assertEquals(
              "803: Access Denied: No permissions for this operation, please add privilege DELETE ON test2.table1",
              e.getMessage());
        }

        // grant and revoke - ANY
        sessionRoot.executeNonQueryStatement("GRANT DELETE ON ANY TO USER userA");
        sessionA.executeNonQueryStatement("DELETE FROM table1");
        sessionRoot.executeNonQueryStatement("REVOKE DELETE ON ANY FROM USER userA");
        try {
          sessionA.executeNonQueryStatement("DELETE FROM table1");
          fail("Should have thrown an exception");
        } catch (StatementExecutionException e) {
          assertEquals(
              "803: Access Denied: No permissions for this operation, please add privilege DELETE ON test2.table1",
              e.getMessage());
        }

        // grant and revoke - database
        sessionRoot.executeNonQueryStatement("GRANT DELETE ON DATABASE test2 TO USER userA");
        sessionA.executeNonQueryStatement("DELETE FROM table1");
        sessionRoot.executeNonQueryStatement("REVOKE DELETE ON DATABASE test2 FROM USER userA");
        try {
          sessionA.executeNonQueryStatement("DELETE FROM table1");
          fail("Should have thrown an exception");
        } catch (StatementExecutionException e) {
          assertEquals(
              "803: Access Denied: No permissions for this operation, please add privilege DELETE ON test2.table1",
              e.getMessage());
        }

        // grant and revoke - table
        sessionRoot.executeNonQueryStatement("GRANT DELETE ON TABLE table1 TO USER userA");
        sessionA.executeNonQueryStatement("DELETE FROM table1");
        sessionRoot.executeNonQueryStatement("REVOKE DELETE ON TABLE table1 FROM USER userA");
        try {
          sessionA.executeNonQueryStatement("DELETE FROM table1");
          fail("Should have thrown an exception");
        } catch (StatementExecutionException e) {
          assertEquals(
              "803: Access Denied: No permissions for this operation, please add privilege DELETE ON test2.table1",
              e.getMessage());
        }

        // grant multiple and revoke one-by-one
        sessionRoot.executeNonQueryStatement("GRANT DELETE ON ANY TO USER userA");
        sessionRoot.executeNonQueryStatement("GRANT DELETE ON DATABASE test2 TO USER userA");
        sessionRoot.executeNonQueryStatement("GRANT DELETE ON TABLE table1 TO USER userA");
        sessionA.executeNonQueryStatement("DELETE FROM table1");
        sessionRoot.executeNonQueryStatement("REVOKE DELETE ON ANY FROM USER userA");
        sessionA.executeNonQueryStatement("DELETE FROM table1");
        sessionRoot.executeNonQueryStatement("REVOKE DELETE ON DATABASE test2 FROM USER userA");
        sessionA.executeNonQueryStatement("DELETE FROM table1");
        sessionRoot.executeNonQueryStatement("REVOKE DELETE ON TABLE table1 FROM USER userA");
        try {
          sessionA.executeNonQueryStatement("DELETE FROM table1");
          fail("Should have thrown an exception");
        } catch (StatementExecutionException e) {
          assertEquals(
              "803: Access Denied: No permissions for this operation, please add privilege DELETE ON test2.table1",
              e.getMessage());
        }

        // userA cannot revoke himself
        sessionRoot.executeNonQueryStatement("GRANT DELETE ON ANY TO USER userA");
        try {
          sessionA.executeNonQueryStatement("REVOKE DELETE ON ANY FROM USER userA");
          fail("Should have thrown an exception");
        } catch (StatementExecutionException e) {
          assertEquals(
              "803: Access Denied: No permissions for this operation, please add grant option to privilege DELETE",
              e.getMessage());
        }
        sessionRoot.executeNonQueryStatement("REVOKE DELETE ON ANY FROM USER userA");

        // userA can revoke himself
        sessionRoot.executeNonQueryStatement("GRANT DELETE ON ANY TO USER userA WITH GRANT OPTION");
        sessionA.executeNonQueryStatement("REVOKE DELETE ON ANY FROM USER userA");
        try {
          sessionA.executeNonQueryStatement("DELETE FROM table1");
          fail("Should have thrown an exception");
        } catch (StatementExecutionException e) {
          assertEquals(
              "803: Access Denied: No permissions for this operation, please add privilege DELETE ON test2.table1",
              e.getMessage());
        }
        // after revoked cannot revoke again
        try {
          sessionA.executeNonQueryStatement("REVOKE DELETE ON ANY FROM USER userA");
          fail("Should have thrown an exception");
        } catch (StatementExecutionException e) {
          assertEquals(
              "803: Access Denied: No permissions for this operation, please add grant option to privilege DELETE",
              e.getMessage());
        }

        // userA cannot grant to userB
        try {
          sessionA.executeNonQueryStatement("GRANT DELETE ON ANY TO USER userB");
          fail("Should have thrown an exception");
        } catch (StatementExecutionException e) {
          assertEquals(
              "803: Access Denied: No permissions for this operation, please add grant option to privilege DELETE",
              e.getMessage());
        }
        sessionRoot.executeNonQueryStatement("GRANT DELETE ON ANY TO USER userA");
        try {
          sessionA.executeNonQueryStatement("GRANT DELETE ON ANY TO USER userB");
          fail("Should have thrown an exception");
        } catch (StatementExecutionException e) {
          assertEquals(
              "803: Access Denied: No permissions for this operation, please add grant option to privilege DELETE",
              e.getMessage());
        }
        try {
          sessionB.executeNonQueryStatement("REVOKE DELETE ON ANY FROM USER userA");
          fail("Should have thrown an exception");
        } catch (StatementExecutionException e) {
          assertEquals(
              "803: Access Denied: No permissions for this operation, please add grant option to privilege DELETE",
              e.getMessage());
        }

        // userA can grant to userB
        sessionRoot.executeNonQueryStatement("GRANT DELETE ON ANY TO USER userA WITH GRANT OPTION");
        sessionA.executeNonQueryStatement("GRANT DELETE ON ANY TO USER userB WITH GRANT OPTION");
        sessionB.executeNonQueryStatement("DELETE FROM table1");
        // userB can revoke userA
        sessionB.executeNonQueryStatement("REVOKE DELETE ON ANY FROM USER userA");
        try {
          sessionA.executeNonQueryStatement("DELETE FROM table1");
          fail("Should have thrown an exception");
        } catch (StatementExecutionException e) {
          assertEquals(
              "803: Access Denied: No permissions for this operation, please add privilege DELETE ON test2.table1",
              e.getMessage());
        }
        // userB can revoke himself
        sessionB.executeNonQueryStatement("REVOKE DELETE ON ANY FROM USER userB");
        try {
          sessionB.executeNonQueryStatement("REVOKE DELETE ON ANY FROM USER userA");
          fail("Should have thrown an exception");
        } catch (StatementExecutionException e) {
          assertEquals(
              "803: Access Denied: No permissions for this operation, please add grant option to privilege DELETE",
              e.getMessage());
        }
      }

      // test role
      sessionRoot.executeNonQueryStatement("CREATE USER userC 'userC1234567'");
      sessionRoot.executeNonQueryStatement("CREATE USER userD 'userD1234567'");
      sessionRoot.executeNonQueryStatement("CREATE ROLE role1");
      sessionRoot.executeNonQueryStatement("CREATE ROLE role2");
      sessionRoot.executeNonQueryStatement("GRANT ROLE role1 TO userC");
      sessionRoot.executeNonQueryStatement("GRANT ROLE role2 TO userD");

      try (ITableSession sessionC =
              EnvFactory.getEnv().getTableSessionConnection("userC", "userC1234567");
          ITableSession sessionD =
              EnvFactory.getEnv().getTableSessionConnection("userD", "userD1234567")) {
        // grant an irrelevant privilege so that the new users can use database
        sessionRoot.executeNonQueryStatement("GRANT SELECT ON DATABASE test2 TO USER userC");
        sessionRoot.executeNonQueryStatement("GRANT SELECT ON DATABASE test2 TO USER userD");
        sessionC.executeNonQueryStatement("USE test2");
        sessionD.executeNonQueryStatement("USE test2");
        // userC no privilege
        try {
          sessionC.executeNonQueryStatement("DELETE FROM table1");
          fail("Should have thrown an exception");
        } catch (StatementExecutionException e) {
          assertEquals(
              "803: Access Denied: No permissions for this operation, please add privilege DELETE ON test2.table1",
              e.getMessage());
        }

        // grant and revoke - ALL
        sessionRoot.executeNonQueryStatement("GRANT ALL TO ROLE role1");
        sessionC.executeNonQueryStatement("DELETE FROM table1");
        sessionRoot.executeNonQueryStatement("REVOKE ALL FROM ROLE role1");
        try {
          sessionC.executeNonQueryStatement("DELETE FROM table1");
          fail("Should have thrown an exception");
        } catch (StatementExecutionException e) {
          assertEquals(
              "803: Access Denied: No permissions for this operation, please add privilege DELETE ON test2.table1",
              e.getMessage());
        }

        // grant and revoke - ANY
        sessionRoot.executeNonQueryStatement("GRANT DELETE ON ANY TO ROLE role1");
        sessionC.executeNonQueryStatement("DELETE FROM table1");
        sessionRoot.executeNonQueryStatement("REVOKE DELETE ON ANY FROM ROLE role1");
        try {
          sessionC.executeNonQueryStatement("DELETE FROM table1");
          fail("Should have thrown an exception");
        } catch (StatementExecutionException e) {
          assertEquals(
              "803: Access Denied: No permissions for this operation, please add privilege DELETE ON test2.table1",
              e.getMessage());
        }

        // grant and revoke - database
        sessionRoot.executeNonQueryStatement("GRANT DELETE ON DATABASE test2 TO ROLE role1");
        sessionC.executeNonQueryStatement("DELETE FROM table1");
        sessionRoot.executeNonQueryStatement("REVOKE DELETE ON DATABASE test2 FROM ROLE role1");
        try {
          sessionC.executeNonQueryStatement("DELETE FROM table1");
          fail("Should have thrown an exception");
        } catch (StatementExecutionException e) {
          assertEquals(
              "803: Access Denied: No permissions for this operation, please add privilege DELETE ON test2.table1",
              e.getMessage());
        }

        // grant and revoke - table
        sessionRoot.executeNonQueryStatement("GRANT DELETE ON TABLE table1 TO ROLE role1");
        sessionC.executeNonQueryStatement("DELETE FROM table1");
        sessionRoot.executeNonQueryStatement("REVOKE DELETE ON TABLE table1 FROM ROLE role1");
        try {
          sessionC.executeNonQueryStatement("DELETE FROM table1");
          fail("Should have thrown an exception");
        } catch (StatementExecutionException e) {
          assertEquals(
              "803: Access Denied: No permissions for this operation, please add privilege DELETE ON test2.table1",
              e.getMessage());
        }

        // grant multiple and revoke one-by-one
        sessionRoot.executeNonQueryStatement("GRANT DELETE ON ANY TO ROLE role1");
        sessionRoot.executeNonQueryStatement("GRANT DELETE ON DATABASE test2 TO ROLE role1");
        sessionRoot.executeNonQueryStatement("GRANT DELETE ON TABLE table1 TO ROLE role1");
        sessionC.executeNonQueryStatement("DELETE FROM table1");
        sessionRoot.executeNonQueryStatement("REVOKE DELETE ON ANY FROM ROLE role1");
        sessionC.executeNonQueryStatement("DELETE FROM table1");
        sessionRoot.executeNonQueryStatement("REVOKE DELETE ON DATABASE test2 FROM ROLE role1");
        sessionC.executeNonQueryStatement("DELETE FROM table1");
        sessionRoot.executeNonQueryStatement("REVOKE DELETE ON TABLE table1 FROM ROLE role1");
        try {
          sessionC.executeNonQueryStatement("DELETE FROM table1");
          fail("Should have thrown an exception");
        } catch (StatementExecutionException e) {
          assertEquals(
              "803: Access Denied: No permissions for this operation, please add privilege DELETE ON test2.table1",
              e.getMessage());
        }

        // role1 cannot revoke himself
        sessionRoot.executeNonQueryStatement("GRANT DELETE ON ANY TO ROLE role1");
        try {
          sessionC.executeNonQueryStatement("REVOKE DELETE ON ANY FROM ROLE role1");
          fail("Should have thrown an exception");
        } catch (StatementExecutionException e) {
          assertEquals(
              "803: Access Denied: No permissions for this operation, please add grant option to privilege DELETE",
              e.getMessage());
        }
        sessionRoot.executeNonQueryStatement("REVOKE DELETE ON ANY FROM ROLE role1");

        // role1 can revoke himself
        sessionRoot.executeNonQueryStatement("GRANT DELETE ON ANY TO ROLE role1 WITH GRANT OPTION");
        sessionC.executeNonQueryStatement("REVOKE DELETE ON ANY FROM ROLE role1");
        try {
          sessionC.executeNonQueryStatement("DELETE FROM table1");
          fail("Should have thrown an exception");
        } catch (StatementExecutionException e) {
          assertEquals(
              "803: Access Denied: No permissions for this operation, please add privilege DELETE ON test2.table1",
              e.getMessage());
        }
        // after revoked cannot revoke again
        try {
          sessionC.executeNonQueryStatement("REVOKE DELETE ON ANY FROM ROLE role1");
          fail("Should have thrown an exception");
        } catch (StatementExecutionException e) {
          assertEquals(
              "803: Access Denied: No permissions for this operation, please add grant option to privilege DELETE",
              e.getMessage());
        }

        // role1 cannot grant to role2
        try {
          sessionC.executeNonQueryStatement("GRANT DELETE ON ANY TO ROLE role2");
          fail("Should have thrown an exception");
        } catch (StatementExecutionException e) {
          assertEquals(
              "803: Access Denied: No permissions for this operation, please add grant option to privilege DELETE",
              e.getMessage());
        }
        sessionRoot.executeNonQueryStatement("GRANT DELETE ON ANY TO ROLE role1");
        try {
          sessionC.executeNonQueryStatement("GRANT DELETE ON ANY TO ROLE role2");
          fail("Should have thrown an exception");
        } catch (StatementExecutionException e) {
          assertEquals(
              "803: Access Denied: No permissions for this operation, please add grant option to privilege DELETE",
              e.getMessage());
        }
        try {
          sessionD.executeNonQueryStatement("DELETE FROM table1");
          fail("Should have thrown an exception");
        } catch (StatementExecutionException e) {
          assertEquals(
              "803: Access Denied: No permissions for this operation, please add privilege DELETE ON test2.table1",
              e.getMessage());
        }

        // role1 can grant to role2
        sessionRoot.executeNonQueryStatement("GRANT DELETE ON ANY TO ROLE role1 WITH GRANT OPTION");
        sessionC.executeNonQueryStatement("GRANT DELETE ON ANY TO ROLE role2 WITH GRANT OPTION");
        sessionD.executeNonQueryStatement("DELETE FROM table1");
        // role2 can revoke role1
        sessionD.executeNonQueryStatement("REVOKE DELETE ON ANY FROM ROLE role1");
        try {
          sessionC.executeNonQueryStatement("DELETE FROM table1");
          fail("Should have thrown an exception");
        } catch (StatementExecutionException e) {
          assertEquals(
              "803: Access Denied: No permissions for this operation, please add privilege DELETE ON test2.table1",
              e.getMessage());
        }
        // role2 can revoke himself
        sessionD.executeNonQueryStatement("REVOKE DELETE ON ANY FROM ROLE role2");
        try {
          sessionD.executeNonQueryStatement("REVOKE DELETE ON ANY FROM ROLE role1");
          fail("Should have thrown an exception");
        } catch (StatementExecutionException e) {
          assertEquals(
              "803: Access Denied: No permissions for this operation, please add grant option to privilege DELETE",
              e.getMessage());
        }

        // lose privilege after role is revoked
        sessionRoot.executeNonQueryStatement("GRANT DELETE ON ANY TO ROLE role1");
        sessionRoot.executeNonQueryStatement("REVOKE ROLE role1 FROM userC");
        try {
          sessionC.executeNonQueryStatement("DELETE FROM table1");
          fail("Should have thrown an exception");
        } catch (StatementExecutionException e) {
          assertEquals(
              "803: Access Denied: No permissions for this operation, please add privilege DELETE ON test2.table1",
              e.getMessage());
        }
      }
    }
  }

  @Test
  public void testTableAuth() throws Exception {
    File tmpDir = new File(Files.createTempDirectory("load").toUri());
    createUser("test", "test123123456");

    final TsFileResource resource4 = new TsFileResource(new File(tmpDir, "test1-0-0-0.tsfile"));
    try (final CompactionTableModelTestFileWriter writer =
        new CompactionTableModelTestFileWriter(resource4)) {
      writer.registerTableSchemaWithTagField("t2", Arrays.asList("id1", "id2", "id3"));
      writer.startChunkGroup("t2", Arrays.asList("id_field1", "id_field2", "id_field3"));
      writer.generateSimpleNonAlignedSeriesToCurrentDevice(
          "s1",
          new TimeRange[][][] {new TimeRange[][] {new TimeRange[] {new TimeRange(20, 22)}}},
          TSEncoding.PLAIN,
          CompressionType.LZ4);
      writer.generateSimpleNonAlignedSeriesToCurrentDevice(
          "s2",
          new TimeRange[][][] {new TimeRange[][] {new TimeRange[] {new TimeRange(20, 22)}}},
          TSEncoding.PLAIN,
          CompressionType.LZ4);
      writer.endChunkGroup();
      writer.endFile();
    }

    try (final Connection userCon =
            EnvFactory.getEnv().getConnection("test", "test123123456", BaseEnv.TABLE_SQL_DIALECT);
        final Statement userStmt = userCon.createStatement()) {
      Assert.assertThrows(
          SQLException.class,
          () -> {
            userStmt.execute(
                String.format(
                    "load '%s' with ('database-level'='2', 'database-name'='test')",
                    tmpDir.getAbsolutePath()));
          });
    }

    try (Connection adminCon = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("grant create on database test to user test");
    }

    try (final Connection userCon =
            EnvFactory.getEnv().getConnection("test", "test123123456", BaseEnv.TABLE_SQL_DIALECT);
        final Statement userStmt = userCon.createStatement()) {
      Assert.assertThrows(
          SQLException.class,
          () -> {
            userStmt.execute(
                String.format(
                    "load '%s' with ('database-level'='2', 'database-name'='test')",
                    tmpDir.getAbsolutePath()));
          });
    }

    try (final Connection adminCon = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("grant insert on any to user test");
    }

    try (final Connection userCon =
            EnvFactory.getEnv().getConnection("test", "test123123456", BaseEnv.TABLE_SQL_DIALECT);
        final Statement userStmt = userCon.createStatement()) {
      userStmt.execute(
          String.format(
              "load '%s' with ('database-level'='2', 'database-name'='test')",
              tmpDir.getAbsolutePath()));
    }
  }
}
