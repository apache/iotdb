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

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Locale;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Table;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.record.Tablet.ColumnCategory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class IoTDBAuthenticationIT {
  @BeforeClass
  public static void setUp() throws Exception {
    Locale.setDefault(Locale.ENGLISH);
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testInsert() throws IoTDBConnectionException, StatementExecutionException {

    try (ITableSession sessionRoot = EnvFactory.getEnv().getTableSessionConnection()) {
      sessionRoot.executeNonQueryStatement("CREATE DATABASE test");
      sessionRoot.executeNonQueryStatement("USE test");

      // insert by root
      Tablet tablet = new Tablet("table1",
          Arrays.asList("id", "attr", "measurement"),
          Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.DOUBLE),
          Arrays.asList(ColumnCategory.ID, ColumnCategory.ATTRIBUTE, ColumnCategory.MEASUREMENT));
      tablet.addTimestamp(0, 0);
      tablet.addValue(0, 0, "id1");
      tablet.addValue(0, 1, "attr1");
      tablet.addValue(0, 0, 0.1);

      sessionRoot.insert(tablet);

      sessionRoot.executeNonQueryStatement("INSERT INTO table1 (time, id, attr, measurement) VALUES (1, 'id2', 'attr2', 0.2)");

      // revoke root
      try {
        sessionRoot.executeNonQueryStatement("REVOKE INSERT ON ANY FROM USER root");
      } catch (StatementExecutionException e) {
        assertEquals("", e.getMessage());
      }
      try {
        sessionRoot.executeNonQueryStatement("REVOKE INSERT ON test FROM USER root");
      } catch (StatementExecutionException e) {
        assertEquals("", e.getMessage());
      }
      try {
        sessionRoot.executeNonQueryStatement("REVOKE INSERT ON table1 FROM USER root");
      } catch (StatementExecutionException e) {
        assertEquals("", e.getMessage());
      }

      // test users
      sessionRoot.executeNonQueryStatement("CREATE USER userA userA");
      sessionRoot.executeNonQueryStatement("CREATE USER userB userB");

      try (ITableSession sessionA = EnvFactory.getEnv().getTableSessionConnection("userA", "userA");
          ITableSession sessionB = EnvFactory.getEnv().getTableSessionConnection("userB", "userB")) {
        sessionA.executeNonQueryStatement("USE test");
        sessionB.executeNonQueryStatement("USE test");
        // userA no privilege
        try {
          sessionA.executeNonQueryStatement("INSERT INTO table1 (time, id, attr, measurement) VALUES (1, 'id2', 'attr2', 0.2)");
        } catch (StatementExecutionException e) {
          assertEquals("", e.getMessage());
        }

        // grant and revoke - ALL
        sessionRoot.executeNonQueryStatement("GRANT ALL ON ANY TO USER userA");
        sessionA.executeNonQueryStatement("INSERT INTO table1 (time, id, attr, measurement) VALUES (1, 'id2', 'attr2', 0.2)");
        sessionRoot.executeNonQueryStatement("REVOKE ALL ON ANY FROM USER userA");
        try {
          sessionA.executeNonQueryStatement("INSERT INTO table1 (time, id, attr, measurement) VALUES (1, 'id2', 'attr2', 0.2)");
        } catch (StatementExecutionException e) {
          assertEquals("", e.getMessage());
        }

        // grant and revoke - ANY
        sessionRoot.executeNonQueryStatement("GRANT INSERT ON ANY TO USER userA");
        sessionA.executeNonQueryStatement("INSERT INTO table1 (time, id, attr, measurement) VALUES (1, 'id2', 'attr2', 0.2)");
        sessionRoot.executeNonQueryStatement("REVOKE INSERT ON ANY FROM USER userA");
        try {
          sessionA.executeNonQueryStatement("INSERT INTO table1 (time, id, attr, measurement) VALUES (1, 'id2', 'attr2', 0.2)");
        } catch (StatementExecutionException e) {
          assertEquals("", e.getMessage());
        }

        // grant and revoke - database
        sessionRoot.executeNonQueryStatement("GRANT INSERT ON test TO USER userA");
        sessionA.executeNonQueryStatement("INSERT INTO table1 (time, id, attr, measurement) VALUES (1, 'id2', 'attr2', 0.2)");
        sessionRoot.executeNonQueryStatement("REVOKE INSERT ON test FROM USER userA");
        try {
          sessionA.executeNonQueryStatement("INSERT INTO table1 (time, id, attr, measurement) VALUES (1, 'id2', 'attr2', 0.2)");
        } catch (StatementExecutionException e) {
          assertEquals("", e.getMessage());
        }

        // grant and revoke - table
        sessionRoot.executeNonQueryStatement("GRANT INSERT ON table1 TO USER userA");
        sessionA.executeNonQueryStatement("INSERT INTO table1 (time, id, attr, measurement) VALUES (1, 'id2', 'attr2', 0.2)");
        sessionRoot.executeNonQueryStatement("REVOKE INSERT ON table1 FROM USER userA");
        try {
          sessionA.executeNonQueryStatement("INSERT INTO table1 (time, id, attr, measurement) VALUES (1, 'id2', 'attr2', 0.2)");
        } catch (StatementExecutionException e) {
          assertEquals("", e.getMessage());
        }

        // grant multiple and revoke one-by-one
        sessionRoot.executeNonQueryStatement("GRANT INSERT ON ANY TO USER userA");
        sessionRoot.executeNonQueryStatement("GRANT INSERT ON test TO USER userA");
        sessionRoot.executeNonQueryStatement("GRANT INSERT ON table1 TO USER userA");
        sessionA.executeNonQueryStatement("INSERT INTO table1 (time, id, attr, measurement) VALUES (1, 'id2', 'attr2', 0.2)");
        sessionRoot.executeNonQueryStatement("REVOKE INSERT ON ANY FROM USER userA");
        sessionA.executeNonQueryStatement("INSERT INTO table1 (time, id, attr, measurement) VALUES (1, 'id2', 'attr2', 0.2)");
        sessionRoot.executeNonQueryStatement("REVOKE INSERT ON test FROM USER userA");
        sessionA.executeNonQueryStatement("INSERT INTO table1 (time, id, attr, measurement) VALUES (1, 'id2', 'attr2', 0.2)");
        sessionRoot.executeNonQueryStatement("REVOKE INSERT ON table1 FROM USER userA");
        try {
          sessionA.executeNonQueryStatement("INSERT INTO table1 (time, id, attr, measurement) VALUES (1, 'id2', 'attr2', 0.2)");
        } catch (StatementExecutionException e) {
          assertEquals("", e.getMessage());
        }

        // userA cannot revoke himself
        sessionRoot.executeNonQueryStatement("GRANT INSERT ON ANY TO USER userA");
        try {
          sessionA.executeNonQueryStatement("REVOKE INSERT ON ANY FROM USER userA");
        } catch (StatementExecutionException e) {
          assertEquals("", e.getMessage());
        }
        sessionRoot.executeNonQueryStatement("REVOKE INSERT ON ANY FROM USER userA");

        // userA can revoke himself
        sessionRoot.executeNonQueryStatement("GRANT INSERT ON ANY TO USER userA WITH GRANT OPTION");
        sessionA.executeNonQueryStatement("REVOKE INSERT ON ANY FROM USER userA");
        try {
          sessionA.executeNonQueryStatement("INSERT INTO table1 (time, id, attr, measurement) VALUES (1, 'id2', 'attr2', 0.2)");
        } catch (StatementExecutionException e) {
          assertEquals("", e.getMessage());
        }
        // after revoked cannot revoke again
        try {
          sessionA.executeNonQueryStatement("REVOKE INSERT ON ANY FROM USER userA");
        } catch (StatementExecutionException e) {
          assertEquals("", e.getMessage());
        }

        // userA cannot grant to userB
        try {
          sessionA.executeNonQueryStatement("GRANT INSERT ON ANY TO USER userB");
        } catch (StatementExecutionException e) {
          assertEquals("", e.getMessage());
        }
        sessionRoot.executeNonQueryStatement("GRANT INSERT ON ANY TO USER userA");
        try {
          sessionA.executeNonQueryStatement("GRANT INSERT ON ANY TO USER userB");
        } catch (StatementExecutionException e) {
          assertEquals("", e.getMessage());
        }
        try {
          sessionB.executeNonQueryStatement("REVOKE INSERT ON ANY FROM USER userA");
        } catch (StatementExecutionException e) {
          assertEquals("", e.getMessage());
        }

        // userA can grant to userB
        sessionRoot.executeNonQueryStatement("GRANT INSERT ON ANY TO USER userA WITH GRANT OPTION");
        sessionA.executeNonQueryStatement("GRANT INSERT ON ANY TO USER userB");
        sessionB.executeNonQueryStatement("INSERT INTO table1 (time, id, attr, measurement) VALUES (1, 'id2', 'attr2', 0.2)");
        // userB can revoke userA
        sessionB.executeNonQueryStatement("REVOKE INSERT ON ANY FROM USER userA");
        try {
          sessionA.executeNonQueryStatement("INSERT INTO table1 (time, id, attr, measurement) VALUES (1, 'id2', 'attr2', 0.2)");
        } catch (StatementExecutionException e) {
          assertEquals("", e.getMessage());
        }
        // userB can revoke himself
        sessionB.executeNonQueryStatement("REVOKE INSERT ON ANY FROM USER userB");
        try {
          sessionB.executeNonQueryStatement("REVOKE INSERT ON ANY FROM USER userA");
        } catch (StatementExecutionException e) {
          assertEquals("", e.getMessage());
        }
      }

      // test role
      sessionRoot.executeNonQueryStatement("CREATE USER userC userC");
      sessionRoot.executeNonQueryStatement("CREATE USER userD userD");
      sessionRoot.executeNonQueryStatement("CREATE ROLE role1");
      sessionRoot.executeNonQueryStatement("CREATE ROLE role2");
      sessionRoot.executeNonQueryStatement("GRANT ROLE role1 TO userC");
      sessionRoot.executeNonQueryStatement("GRANT ROLE role2 TO userD");

      try (ITableSession sessionC = EnvFactory.getEnv().getTableSessionConnection("userC", "userC");
          ITableSession sessionD = EnvFactory.getEnv().getTableSessionConnection("userD", "userD")) {
        sessionC.executeNonQueryStatement("USE test");
        sessionD.executeNonQueryStatement("USE test");
        // userC no privilege
        try {
          sessionC.executeNonQueryStatement("INSERT INTO table1 (time, id, attr, measurement) VALUES (1, 'id2', 'attr2', 0.2)");
        } catch (StatementExecutionException e) {
          assertEquals("", e.getMessage());
        }

        // grant and revoke - ALL
        sessionRoot.executeNonQueryStatement("GRANT ALL ON ANY TO ROLE role1");
        sessionC.executeNonQueryStatement("INSERT INTO table1 (time, id, attr, measurement) VALUES (1, 'id2', 'attr2', 0.2)");
        sessionRoot.executeNonQueryStatement("REVOKE ALL ON ANY FROM ROLE role1");
        try {
          sessionC.executeNonQueryStatement("INSERT INTO table1 (time, id, attr, measurement) VALUES (1, 'id2', 'attr2', 0.2)");
        } catch (StatementExecutionException e) {
          assertEquals("", e.getMessage());
        }

        // grant and revoke - ANY
        sessionRoot.executeNonQueryStatement("GRANT INSERT ON ANY TO ROLE role1");
        sessionC.executeNonQueryStatement("INSERT INTO table1 (time, id, attr, measurement) VALUES (1, 'id2', 'attr2', 0.2)");
        sessionRoot.executeNonQueryStatement("REVOKE INSERT ON ANY FROM ROLE role1");
        try {
          sessionC.executeNonQueryStatement("INSERT INTO table1 (time, id, attr, measurement) VALUES (1, 'id2', 'attr2', 0.2)");
        } catch (StatementExecutionException e) {
          assertEquals("", e.getMessage());
        }

        // grant and revoke - database
        sessionRoot.executeNonQueryStatement("GRANT INSERT ON test TO ROLE role1");
        sessionC.executeNonQueryStatement("INSERT INTO table1 (time, id, attr, measurement) VALUES (1, 'id2', 'attr2', 0.2)");
        sessionRoot.executeNonQueryStatement("REVOKE INSERT ON test FROM ROLE role1");
        try {
          sessionC.executeNonQueryStatement("INSERT INTO table1 (time, id, attr, measurement) VALUES (1, 'id2', 'attr2', 0.2)");
        } catch (StatementExecutionException e) {
          assertEquals("", e.getMessage());
        }

        // grant and revoke - table
        sessionRoot.executeNonQueryStatement("GRANT INSERT ON table1 TO ROLE role1");
        sessionC.executeNonQueryStatement("INSERT INTO table1 (time, id, attr, measurement) VALUES (1, 'id2', 'attr2', 0.2)");
        sessionRoot.executeNonQueryStatement("REVOKE INSERT ON table1 FROM ROLE role1");
        try {
          sessionC.executeNonQueryStatement("INSERT INTO table1 (time, id, attr, measurement) VALUES (1, 'id2', 'attr2', 0.2)");
        } catch (StatementExecutionException e) {
          assertEquals("", e.getMessage());
        }

        // grant multiple and revoke one-by-one
        sessionRoot.executeNonQueryStatement("GRANT INSERT ON ANY TO ROLE role1");
        sessionRoot.executeNonQueryStatement("GRANT INSERT ON test TO ROLE role1");
        sessionRoot.executeNonQueryStatement("GRANT INSERT ON table1 TO ROLE role1");
        sessionC.executeNonQueryStatement("INSERT INTO table1 (time, id, attr, measurement) VALUES (1, 'id2', 'attr2', 0.2)");
        sessionRoot.executeNonQueryStatement("REVOKE INSERT ON ANY FROM ROLE role1");
        sessionC.executeNonQueryStatement("INSERT INTO table1 (time, id, attr, measurement) VALUES (1, 'id2', 'attr2', 0.2)");
        sessionRoot.executeNonQueryStatement("REVOKE INSERT ON test FROM ROLE role1");
        sessionC.executeNonQueryStatement("INSERT INTO table1 (time, id, attr, measurement) VALUES (1, 'id2', 'attr2', 0.2)");
        sessionRoot.executeNonQueryStatement("REVOKE INSERT ON table1 FROM ROLE role1");
        try {
          sessionC.executeNonQueryStatement("INSERT INTO table1 (time, id, attr, measurement) VALUES (1, 'id2', 'attr2', 0.2)");
        } catch (StatementExecutionException e) {
          assertEquals("", e.getMessage());
        }

        // role1 cannot revoke himself
        sessionRoot.executeNonQueryStatement("GRANT INSERT ON ANY TO ROLE role1");
        try {
          sessionC.executeNonQueryStatement("REVOKE INSERT ON ANY FROM ROLE role1");
        } catch (StatementExecutionException e) {
          assertEquals("", e.getMessage());
        }
        sessionRoot.executeNonQueryStatement("REVOKE INSERT ON ANY FROM ROLE role1");

        // role1 can revoke himself
        sessionRoot.executeNonQueryStatement("GRANT INSERT ON ANY TO USER userC WITH GRANT OPTION");
        sessionC.executeNonQueryStatement("REVOKE INSERT ON ANY FROM ROLE role1");
        try {
          sessionC.executeNonQueryStatement("INSERT INTO table1 (time, id, attr, measurement) VALUES (1, 'id2', 'attr2', 0.2)");
        } catch (StatementExecutionException e) {
          assertEquals("", e.getMessage());
        }
        // after revoked cannot revoke again
        try {
          sessionC.executeNonQueryStatement("REVOKE INSERT ON ANY FROM ROLE role1");
        } catch (StatementExecutionException e) {
          assertEquals("", e.getMessage());
        }

        // role1 cannot grant to role2
        try {
          sessionC.executeNonQueryStatement("GRANT INSERT ON ANY TO ROLE role2");
        } catch (StatementExecutionException e) {
          assertEquals("", e.getMessage());
        }
        sessionRoot.executeNonQueryStatement("GRANT INSERT ON ANY TO ROLE role1");
        try {
          sessionC.executeNonQueryStatement("GRANT INSERT ON ANY TO ROLE role2");
        } catch (StatementExecutionException e) {
          assertEquals("", e.getMessage());
        }
        try {
          sessionD.executeNonQueryStatement("INSERT INTO table1 (time, id, attr, measurement) VALUES (1, 'id2', 'attr2', 0.2)");
        } catch (StatementExecutionException e) {
          assertEquals("", e.getMessage());
        }

        // userC can grant to userD
        sessionRoot.executeNonQueryStatement("GRANT INSERT ON ANY TO USER userC WITH GRANT OPTION");
        sessionC.executeNonQueryStatement("GRANT INSERT ON ANY TO ROLE role2");
        sessionD.executeNonQueryStatement("INSERT INTO table1 (time, id, attr, measurement) VALUES (1, 'id2', 'attr2', 0.2)");
        // userD can revoke userC
        sessionD.executeNonQueryStatement("REVOKE INSERT ON ANY FROM ROLE role1");
        try {
          sessionC.executeNonQueryStatement("INSERT INTO table1 (time, id, attr, measurement) VALUES (1, 'id2', 'attr2', 0.2)");
        } catch (StatementExecutionException e) {
          assertEquals("", e.getMessage());
        }
        // userD can revoke himself
        sessionD.executeNonQueryStatement("REVOKE INSERT ON ANY FROM ROLE role2");
        try {
          sessionD.executeNonQueryStatement("REVOKE INSERT ON ANY FROM ROLE role1");
        } catch (StatementExecutionException e) {
          assertEquals("", e.getMessage());
        }

        // lose privilege after role is revoked
        sessionRoot.executeNonQueryStatement("GRANT INSERT ON ANY TO ROLE role1");
        sessionRoot.executeNonQueryStatement("REVOKE ROLE role1 FROM userC");
        try {
          sessionC.executeNonQueryStatement("INSERT INTO table1 (time, id, attr, measurement) VALUES (1, 'id2', 'attr2', 0.2)");
        } catch (StatementExecutionException e) {
          assertEquals("", e.getMessage());
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
      Tablet tablet = new Tablet("table1",
          Arrays.asList("id", "attr", "measurement"),
          Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.DOUBLE),
          Arrays.asList(ColumnCategory.ID, ColumnCategory.ATTRIBUTE, ColumnCategory.MEASUREMENT));
      tablet.addTimestamp(0, 0);
      tablet.addValue(0, 0, "id1");
      tablet.addValue(0, 1, "attr1");
      tablet.addValue(0, 0, 0.1);

      sessionRoot.insert(tablet);

      sessionRoot.executeNonQueryStatement("DELETE FROM table1");

      // revoke root
      try {
        sessionRoot.executeNonQueryStatement("REVOKE DELETE ON ANY FROM USER root");
      } catch (StatementExecutionException e) {
        assertEquals("", e.getMessage());
      }
      try {
        sessionRoot.executeNonQueryStatement("REVOKE DELETE ON test FROM USER root");
      } catch (StatementExecutionException e) {
        assertEquals("", e.getMessage());
      }
      try {
        sessionRoot.executeNonQueryStatement("REVOKE DELETE ON table1 FROM USER root");
      } catch (StatementExecutionException e) {
        assertEquals("", e.getMessage());
      }

      // test users
      sessionRoot.executeNonQueryStatement("CREATE USER userA userA");
      sessionRoot.executeNonQueryStatement("CREATE USER userB userB");

      try (ITableSession sessionA = EnvFactory.getEnv().getTableSessionConnection("userA", "userA");
          ITableSession sessionB = EnvFactory.getEnv().getTableSessionConnection("userB", "userB")) {
        sessionA.executeNonQueryStatement("USE test2");
        sessionB.executeNonQueryStatement("USE test2");
        // userA no privilege
        try {
          sessionA.executeNonQueryStatement("DELETE FROM table1");
        } catch (StatementExecutionException e) {
          assertEquals("", e.getMessage());
        }

        // grant and revoke - ALL
        sessionRoot.executeNonQueryStatement("GRANT ALL ON ANY TO USER userA");
        sessionA.executeNonQueryStatement("DELETE FROM table1");
        sessionRoot.executeNonQueryStatement("REVOKE ALL ON ANY FROM USER userA");
        try {
          sessionA.executeNonQueryStatement("DELETE FROM table1");
        } catch (StatementExecutionException e) {
          assertEquals("", e.getMessage());
        }

        // grant and revoke - ANY
        sessionRoot.executeNonQueryStatement("GRANT DELETE ON ANY TO USER userA");
        sessionA.executeNonQueryStatement("DELETE FROM table1");
        sessionRoot.executeNonQueryStatement("REVOKE DELETE ON ANY FROM USER userA");
        try {
          sessionA.executeNonQueryStatement("DELETE FROM table1");
        } catch (StatementExecutionException e) {
          assertEquals("", e.getMessage());
        }

        // grant and revoke - database
        sessionRoot.executeNonQueryStatement("GRANT DELETE ON test TO USER userA");
        sessionA.executeNonQueryStatement("DELETE FROM table1");
        sessionRoot.executeNonQueryStatement("REVOKE DELETE ON test FROM USER userA");
        try {
          sessionA.executeNonQueryStatement("DELETE FROM table1");
        } catch (StatementExecutionException e) {
          assertEquals("", e.getMessage());
        }

        // grant and revoke - table
        sessionRoot.executeNonQueryStatement("GRANT DELETE ON table1 TO USER userA");
        sessionA.executeNonQueryStatement("DELETE FROM table1");
        sessionRoot.executeNonQueryStatement("REVOKE DELETE ON table1 FROM USER userA");
        try {
          sessionA.executeNonQueryStatement("DELETE FROM table1");
        } catch (StatementExecutionException e) {
          assertEquals("", e.getMessage());
        }

        // grant multiple and revoke one-by-one
        sessionRoot.executeNonQueryStatement("GRANT DELETE ON ANY TO USER userA");
        sessionRoot.executeNonQueryStatement("GRANT DELETE ON test TO USER userA");
        sessionRoot.executeNonQueryStatement("GRANT DELETE ON table1 TO USER userA");
        sessionA.executeNonQueryStatement("DELETE FROM table1");
        sessionRoot.executeNonQueryStatement("REVOKE DELETE ON ANY FROM USER userA");
        sessionA.executeNonQueryStatement("DELETE FROM table1");
        sessionRoot.executeNonQueryStatement("REVOKE DELETE ON test FROM USER userA");
        sessionA.executeNonQueryStatement("DELETE FROM table1");
        sessionRoot.executeNonQueryStatement("REVOKE DELETE ON table1 FROM USER userA");
        try {
          sessionA.executeNonQueryStatement("DELETE FROM table1");
        } catch (StatementExecutionException e) {
          assertEquals("", e.getMessage());
        }

        // userA cannot revoke himself
        sessionRoot.executeNonQueryStatement("GRANT DELETE ON ANY TO USER userA");
        try {
          sessionA.executeNonQueryStatement("REVOKE DELETE ON ANY FROM USER userA");
        } catch (StatementExecutionException e) {
          assertEquals("", e.getMessage());
        }
        sessionRoot.executeNonQueryStatement("REVOKE DELETE ON ANY FROM USER userA");

        // userA can revoke himself
        sessionRoot.executeNonQueryStatement("GRANT DELETE ON ANY TO USER userA WITH GRANT OPTION");
        sessionA.executeNonQueryStatement("REVOKE DELETE ON ANY FROM USER userA");
        try {
          sessionA.executeNonQueryStatement("DELETE FROM table1");
        } catch (StatementExecutionException e) {
          assertEquals("", e.getMessage());
        }
        // after revoked cannot revoke again
        try {
          sessionA.executeNonQueryStatement("REVOKE DELETE ON ANY FROM USER userA");
        } catch (StatementExecutionException e) {
          assertEquals("", e.getMessage());
        }

        // userA cannot grant to userB
        try {
          sessionA.executeNonQueryStatement("GRANT DELETE ON ANY TO USER userB");
        } catch (StatementExecutionException e) {
          assertEquals("", e.getMessage());
        }
        sessionRoot.executeNonQueryStatement("GRANT DELETE ON ANY TO USER userA");
        try {
          sessionA.executeNonQueryStatement("GRANT DELETE ON ANY TO USER userB");
        } catch (StatementExecutionException e) {
          assertEquals("", e.getMessage());
        }
        try {
          sessionB.executeNonQueryStatement("REVOKE DELETE ON ANY FROM USER userA");
        } catch (StatementExecutionException e) {
          assertEquals("", e.getMessage());
        }

        // userA can grant to userB
        sessionRoot.executeNonQueryStatement("GRANT DELETE ON ANY TO USER userA WITH GRANT OPTION");
        sessionA.executeNonQueryStatement("GRANT DELETE ON ANY TO USER userB");
        sessionB.executeNonQueryStatement("DELETE FROM table1");
        // userB can revoke userA
        sessionB.executeNonQueryStatement("REVOKE DELETE ON ANY FROM USER userA");
        try {
          sessionA.executeNonQueryStatement("DELETE FROM table1");
        } catch (StatementExecutionException e) {
          assertEquals("", e.getMessage());
        }
        // userB can revoke himself
        sessionB.executeNonQueryStatement("REVOKE DELETE ON ANY FROM USER userB");
        try {
          sessionB.executeNonQueryStatement("REVOKE DELETE ON ANY FROM USER userA");
        } catch (StatementExecutionException e) {
          assertEquals("", e.getMessage());
        }
      }

      // test role
      sessionRoot.executeNonQueryStatement("CREATE USER userC userC");
      sessionRoot.executeNonQueryStatement("CREATE USER userD userD");
      sessionRoot.executeNonQueryStatement("CREATE ROLE role1");
      sessionRoot.executeNonQueryStatement("CREATE ROLE role2");
      sessionRoot.executeNonQueryStatement("GRANT ROLE role1 TO userC");
      sessionRoot.executeNonQueryStatement("GRANT ROLE role2 TO userD");

      try (ITableSession sessionC = EnvFactory.getEnv().getTableSessionConnection("userC", "userC");
          ITableSession sessionD = EnvFactory.getEnv().getTableSessionConnection("userD", "userD")) {
        sessionC.executeNonQueryStatement("USE test");
        // userC no privilege
        try {
          sessionC.executeNonQueryStatement("DELETE FROM table1");
        } catch (StatementExecutionException e) {
          assertEquals("", e.getMessage());
        }

        // grant and revoke - ALL
        sessionRoot.executeNonQueryStatement("GRANT ALL ON ANY TO ROLE role1");
        sessionC.executeNonQueryStatement("DELETE FROM table1");
        sessionRoot.executeNonQueryStatement("REVOKE ALL ON ANY FROM ROLE role1");
        try {
          sessionC.executeNonQueryStatement("DELETE FROM table1");
        } catch (StatementExecutionException e) {
          assertEquals("", e.getMessage());
        }

        // grant and revoke - ANY
        sessionRoot.executeNonQueryStatement("GRANT DELETE ON ANY TO ROLE role1");
        sessionC.executeNonQueryStatement("DELETE FROM table1");
        sessionRoot.executeNonQueryStatement("REVOKE DELETE ON ANY FROM ROLE role1");
        try {
          sessionC.executeNonQueryStatement("DELETE FROM table1");
        } catch (StatementExecutionException e) {
          assertEquals("", e.getMessage());
        }

        // grant and revoke - database
        sessionRoot.executeNonQueryStatement("GRANT DELETE ON test TO ROLE role1");
        sessionC.executeNonQueryStatement("DELETE FROM table1");
        sessionRoot.executeNonQueryStatement("REVOKE DELETE ON test FROM ROLE role1");
        try {
          sessionC.executeNonQueryStatement("DELETE FROM table1");
        } catch (StatementExecutionException e) {
          assertEquals("", e.getMessage());
        }

        // grant and revoke - table
        sessionRoot.executeNonQueryStatement("GRANT DELETE ON table1 TO ROLE role1");
        sessionC.executeNonQueryStatement("DELETE FROM table1");
        sessionRoot.executeNonQueryStatement("REVOKE DELETE ON table1 FROM ROLE role1");
        try {
          sessionC.executeNonQueryStatement("DELETE FROM table1");
        } catch (StatementExecutionException e) {
          assertEquals("", e.getMessage());
        }

        // grant multiple and revoke one-by-one
        sessionRoot.executeNonQueryStatement("GRANT DELETE ON ANY TO ROLE role1");
        sessionRoot.executeNonQueryStatement("GRANT DELETE ON test TO ROLE role1");
        sessionRoot.executeNonQueryStatement("GRANT DELETE ON table1 TO ROLE role1");
        sessionC.executeNonQueryStatement("DELETE FROM table1");
        sessionRoot.executeNonQueryStatement("REVOKE DELETE ON ANY FROM ROLE role1");
        sessionC.executeNonQueryStatement("DELETE FROM table1");
        sessionRoot.executeNonQueryStatement("REVOKE DELETE ON test FROM ROLE role1");
        sessionC.executeNonQueryStatement("DELETE FROM table1");
        sessionRoot.executeNonQueryStatement("REVOKE DELETE ON table1 FROM ROLE role1");
        try {
          sessionC.executeNonQueryStatement("DELETE FROM table1");
        } catch (StatementExecutionException e) {
          assertEquals("", e.getMessage());
        }

        // role1 cannot revoke himself
        sessionRoot.executeNonQueryStatement("GRANT DELETE ON ANY TO ROLE role1");
        try {
          sessionC.executeNonQueryStatement("REVOKE DELETE ON ANY FROM ROLE role1");
        } catch (StatementExecutionException e) {
          assertEquals("", e.getMessage());
        }
        sessionRoot.executeNonQueryStatement("REVOKE DELETE ON ANY FROM ROLE role1");

        // role1 can revoke himself
        sessionRoot.executeNonQueryStatement("GRANT DELETE ON ANY TO USER userC WITH GRANT OPTION");
        sessionC.executeNonQueryStatement("REVOKE DELETE ON ANY FROM ROLE role1");
        try {
          sessionC.executeNonQueryStatement("DELETE FROM table1");
        } catch (StatementExecutionException e) {
          assertEquals("", e.getMessage());
        }
        // after revoked cannot revoke again
        try {
          sessionC.executeNonQueryStatement("REVOKE DELETE ON ANY FROM ROLE role1");
        } catch (StatementExecutionException e) {
          assertEquals("", e.getMessage());
        }

        // role1 cannot grant to role2
        try {
          sessionC.executeNonQueryStatement("GRANT DELETE ON ANY TO ROLE role2");
        } catch (StatementExecutionException e) {
          assertEquals("", e.getMessage());
        }
        sessionRoot.executeNonQueryStatement("GRANT DELETE ON ANY TO ROLE role1");
        try {
          sessionC.executeNonQueryStatement("GRANT DELETE ON ANY TO ROLE role2");
        } catch (StatementExecutionException e) {
          assertEquals("", e.getMessage());
        }
        try {
          sessionD.executeNonQueryStatement("DELETE FROM table1");
        } catch (StatementExecutionException e) {
          assertEquals("", e.getMessage());
        }

        // userC can grant to userD
        sessionRoot.executeNonQueryStatement("GRANT DELETE ON ANY TO USER userC WITH GRANT OPTION");
        sessionC.executeNonQueryStatement("GRANT DELETE ON ANY TO ROLE role2");
        sessionD.executeNonQueryStatement("DELETE FROM table1");
        // userD can revoke userC
        sessionD.executeNonQueryStatement("REVOKE DELETE ON ANY FROM ROLE role1");
        try {
          sessionC.executeNonQueryStatement("DELETE FROM table1");
        } catch (StatementExecutionException e) {
          assertEquals("", e.getMessage());
        }
        // userD can revoke himself
        sessionD.executeNonQueryStatement("REVOKE DELETE ON ANY FROM ROLE role2");
        try {
          sessionD.executeNonQueryStatement("REVOKE DELETE ON ANY FROM ROLE role1");
        } catch (StatementExecutionException e) {
          assertEquals("", e.getMessage());
        }

        // lose privilege after role is revoked
        sessionRoot.executeNonQueryStatement("GRANT DELETE ON ANY TO ROLE role1");
        sessionRoot.executeNonQueryStatement("REVOKE ROLE role1 FROM userC");
        try {
          sessionC.executeNonQueryStatement("DELETE FROM table1");
        } catch (StatementExecutionException e) {
          assertEquals("", e.getMessage());
        }
      }
    }
  }

}
