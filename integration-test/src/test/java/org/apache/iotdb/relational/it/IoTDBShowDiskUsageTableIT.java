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

package org.apache.iotdb.relational.it;

import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBShowDiskUsageTableIT {

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("create database test");
      session.executeNonQueryStatement("use test");
      session.executeNonQueryStatement(
          "create table t1(tag1 string tag, s0 int32 field, s1 int32 field)");
      session.executeNonQueryStatement(
          "create table t2(tag1 string tag, s0 int32 field, s1 int32 field)");
      session.executeNonQueryStatement(
          "create view v1(tag1 string tag, s0 int32 field, s1 int32 field) as root.test.**");
      for (int i = 0; i < 20; i++) {
        session.executeNonQueryStatement(
            "insert into t1(time,tag1,s0,s1) values (" + i + ", 'd" + i + "', 1, 1)");
      }
      session.executeNonQueryStatement("insert into t1(time,tag1,s0,s1) values (-1,'d1',1,1)");
      session.executeNonQueryStatement("flush");
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void test1() {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      SessionDataSet sessionDataSet =
          session.executeQueryStatement(
              "select * from information_schema.table_disk_usage where database = 'test'");
      SessionDataSet.DataIterator iterator = sessionDataSet.iterator();
      long sum = 0;
      Map<Long, Long> timePartitionSizes = new HashMap<>();
      Map<String, Long> tableSizes = new HashMap<>();
      while (iterator.next()) {
        String table = iterator.getString("table_name");
        long timePartition = iterator.getLong("time_partition");
        long size = iterator.getLong("size_in_bytes");
        timePartitionSizes.compute(timePartition, (k, v) -> v == null ? size : v + size);
        tableSizes.compute(table, (k, v) -> v == null ? size : v + size);
        sum += size;
      }
      Assert.assertTrue(sum > 0);
      Assert.assertEquals(2, tableSizes.size());
      Assert.assertEquals(0L, (long) tableSizes.get("t2"));
      Assert.assertFalse(tableSizes.containsKey("v1"));
      Assert.assertTrue(timePartitionSizes.containsKey(0L));
      Assert.assertTrue(timePartitionSizes.containsKey(-1L));
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void test2() {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      SessionDataSet sessionDataSet =
          session.executeQueryStatement(
              "select * from information_schema.table_disk_usage where database = 'test' limit 2");
      SessionDataSet.DataIterator iterator = sessionDataSet.iterator();
      int count = 0;
      while (iterator.next()) {
        count++;
      }
      Assert.assertEquals(2, count);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void test3() {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      SessionDataSet sessionDataSet =
          session.executeQueryStatement(
              "select * from information_schema.table_disk_usage where database = 'test' order by time_partition desc");
      SessionDataSet.DataIterator iterator = sessionDataSet.iterator();
      long previousTimePartition = Long.MAX_VALUE;
      while (iterator.next()) {
        long currentTimePartition = iterator.getLong("time_partition");
        Assert.assertTrue(currentTimePartition <= previousTimePartition);
        previousTimePartition = currentTimePartition;
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void test4() {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      SessionDataSet sessionDataSet =
          session.executeQueryStatement(
              "select * from information_schema.table_disk_usage where database = 'test' and (time_partition < 0 or datanode_id >= 2)");
      SessionDataSet.DataIterator iterator = sessionDataSet.iterator();
      while (iterator.next()) {
        Assert.assertTrue(
            iterator.getLong("time_partition") < 0 || iterator.getLong("datanode_id") >= 2);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void test5() {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      SessionDataSet sessionDataSet =
          session.executeQueryStatement(
              "select * from information_schema.table_disk_usage where database = 'test' order by time_partition desc limit 2");
      SessionDataSet.DataIterator iterator = sessionDataSet.iterator();
      long previousTimePartition = Long.MAX_VALUE;
      int count = 0;
      while (iterator.next()) {
        count++;
        long currentTimePartition = iterator.getLong("time_partition");
        Assert.assertTrue(currentTimePartition <= previousTimePartition);
        previousTimePartition = currentTimePartition;
      }
      Assert.assertEquals(2, count);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
