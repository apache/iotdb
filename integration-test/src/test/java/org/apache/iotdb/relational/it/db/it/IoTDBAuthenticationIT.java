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
    // insert by root
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("CREATE DATABASE test");
      session.executeNonQueryStatement("USE test");

      Tablet tablet = new Tablet("table1",
          Arrays.asList("id", "attr", "measurement"),
          Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.DOUBLE),
          Arrays.asList(ColumnCategory.ID, ColumnCategory.ATTRIBUTE, ColumnCategory.MEASUREMENT));
      tablet.addTimestamp(0, 0);
      tablet.addValue(0, 0, "id1");
      tablet.addValue(0, 1, "attr1");
      tablet.addValue(0, 0, 0.1);

      session.insert(tablet);

      session.executeNonQueryStatement("INSERT INTO table1 (time, id, attr, measurement) VALUES (1, 'id2', 'attr2', 0.2)");
    }
  }
}
