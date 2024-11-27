/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.relational.it.db.it.udf.scalar;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.Statement;

import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBScalarFunctionIT {
    private static String[] sqls =
            new String[] {
                    "CREATE DATABASE test",
                    "USE \"test\"",
                    "CREATE TABLE vehicle (device_id string id, s1 INT32 measurement, s2 INT64 measurement, s3 FLOAT MEASUREMENT, s4 DOUBLE MEASUREMENT, s5 BOOLEAN MEASUREMENT)",
                    "insert into vehicle(time, device_id, s1, s2, s3, s4, s5) values (1, 'd0', 1, 1, 1.1, 1.1, true)",
                    "insert into vehicle(time, device_id, s1, s2, s3, s4, s5) values (2, 'd0', null, 2, 2.2, 2.2, true)",
                    "insert into vehicle(time, device_id, s1, s2, s3, s4, s5) values (3, 'd0', 3, 3, null, null, false)",
                    "insert into vehicle(time, device_id, s5) values (5, 'd0', true)",
            };

    @Before
    public static void setUp() throws Exception {
        EnvFactory.getEnv().initClusterEnvironment();
        insertData();
    }

    @After
    public static void tearDown() throws Exception {
        EnvFactory.getEnv().cleanClusterEnvironment();
    }

    private static void insertData() {
        try (Connection connection = EnvFactory.getEnv().getTableConnection();
             Statement statement = connection.createStatement()) {
            for (String sql : sqls) {
                System.out.println(sql);
                statement.execute(sql);
            }
        } catch (Exception e) {
            fail("insertData failed.");
        }
    }

    @Test
    public void testIllegalInput(){
        try (Connection connection = EnvFactory.getEnv().getTableConnection();
             Statement statement = connection.createStatement()) {
            statement.execute("CREATE FUNCTION contain_null as 'org.apache.iotdb.db.query.udf.example.relational.ContainNull'");
            statement.execute("CREATE FUNCTION all_sum as 'org.apache.iotdb.db.query.udf.example.relational.AllSum'");
        } catch (Exception e) {
            fail("insertData failed.");
        }
    }


}
