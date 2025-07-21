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

package org.apache.iotdb.relational.it.query.recent;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareTableData;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBComplexQueryIT {
  protected static final String DATABASE_NAME = "test_db";
  protected static final String[] createSqls =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "create table employees(department_id STRING TAG,remark STRING ATTRIBUTE,name TEXT FIELD,Gender TEXT FIELD,Status BOOLEAN FIELD,employee_id INT32 FIELD,salary DOUBLE FIELD,date_of_birth DATE FIELD,Contac_info string FIELD)",
        "create table departments(department_id STRING TAG,dep_description STRING ATTRIBUTE,dep_name TEXT FIELD,dep_phone TEXT FIELD,dep_status BOOLEAN FIELD,dep_member INT32 FIELD,employee_id INT32 FIELD)",
        "insert into employees(time, department_id, remark, name, gender, status, employee_id, salary, date_of_birth, contac_info) values(1, 'D001', 'good', 'Mary','Female', false, 1223, 5500.22, '1988-10-12', '133-1212-1234')",
        "insert into employees(time, department_id, remark, name, gender, status, employee_id, salary, date_of_birth, contac_info) values(2, 'D001', 'great', 'John', 'Male', true, 40012, 8822, '1985-06-15', '130-1002-1334')",
        "insert into employees(time, department_id, remark, name, gender, status, employee_id, salary, date_of_birth, contac_info) values(3, 'D002', 'excellent', 'Nancy', 'Female', true, 30112, 10002, '1983-08-15', '135-1302-1354')",
        "insert into employees(time, department_id, remark, name, gender, status, employee_id, salary, date_of_birth, contac_info) values(4, 'D002', 'good', 'Jack', 'Male', false, 12212, 7000, '1990-03-26', '138-1012-1353')",
        "insert into employees(time, department_id, remark, name, gender, status, employee_id, salary, date_of_birth, contac_info) values(5, 'D003', 'great', 'Linda', 'Female', false, 10212, 5600, '1995-06-15', '150-2003-1355')",
        "insert into departments(time, department_id, dep_description, dep_name, dep_phone, dep_status, dep_member,employee_id) values(1, 'D001', 'goods','销售部', '010-2271-2120', false, 1223,1223)",
        "insert into departments(time, department_id, dep_description, dep_name, dep_phone, dep_status, dep_member,employee_id) values(2, 'D001', 'goods','销售部', '010-2271-2120', false, 102, 40012)",
        "insert into departments(time, department_id, dep_description, dep_name, dep_phone, dep_status, dep_member,employee_id) values(3, 'D002', 'service','客服部', '010-2077-2520', true, 220, 30112)",
        "insert into departments(time, department_id, dep_description, dep_name, dep_phone, dep_status, dep_member,employee_id) values(4, 'D002', 'service','客服部', '010-2077-2520', true, 2012, 12212)",
        "insert into departments(time, department_id, dep_description, dep_name, dep_phone, dep_status, dep_member,employee_id) values(5, 'D003', 'IT','研发部', '010-3272-2310', true, 300, 10212)",
        "insert into departments(time, department_id, dep_description, dep_name, dep_phone, dep_status, dep_member,employee_id) values(6, 'D004', 'IT','人事部', '010-3272-2312', true, 300, 10200)",
        "FLUSH",
        "CLEAR ATTRIBUTE CACHE",
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    prepareTableData(createSqls);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void queryTest1() {
    // Look for the non-intersecting departments in the two tables
    String[] expectedHeader = new String[] {"department_id", "dep_name"};
    String[] retArray = new String[] {"D004,人事部,"};
    tableResultSetEqualTest(
        "select department_id, dep_name from departments where not exists("
            + "select 1 from employees where employees.department_id = departments.department_id)",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }
}
