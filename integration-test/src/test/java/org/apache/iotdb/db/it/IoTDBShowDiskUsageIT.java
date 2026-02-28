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

package org.apache.iotdb.db.it;

import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

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
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBShowDiskUsageIT {

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();

    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      for (int i = 0; i < 20; i++) {
        session.executeNonQueryStatement(
            "insert into root.test.d" + i + "(time, s0, s1, s2) values (" + i + ",1, 1, 1)");
      }
      session.executeNonQueryStatement(
          "insert into root.test.d0(time,s0,s1) aligned values (-1,1,1)");
      session.executeNonQueryStatement("flush");
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void test1() {

    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      SessionDataSet sessionDataSet =
          session.executeQueryStatement("show disk_usage from root.test.**");
      SessionDataSet.DataIterator iterator = sessionDataSet.iterator();
      long sum = 0;
      Map<Long, Long> timePartitionSizes = new HashMap<>();
      while (iterator.next()) {
        long timePartition = iterator.getLong("TimePartition");
        long size = iterator.getLong("SizeInBytes");
        timePartitionSizes.compute(timePartition, (k, v) -> v == null ? size : v + size);
        sum += size;
      }
      Assert.assertTrue(sum > 0);
      Assert.assertTrue(timePartitionSizes.containsKey(0L));
      Assert.assertTrue(timePartitionSizes.containsKey(-1L));
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void test2() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      SessionDataSet sessionDataSet =
          session.executeQueryStatement("show disk_usage from root.test.** limit 2");
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
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      SessionDataSet sessionDataSet =
          session.executeQueryStatement(
              "show disk_usage from root.test.** order by TimePartition desc");
      SessionDataSet.DataIterator iterator = sessionDataSet.iterator();
      long previousTimePartition = Long.MAX_VALUE;
      while (iterator.next()) {
        long currentTimePartition = iterator.getLong("TimePartition");
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
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      SessionDataSet sessionDataSet =
          session.executeQueryStatement(
              "show disk_usage from root.test.** where TimePartition < 0 or DataNodeId >= 2");
      SessionDataSet.DataIterator iterator = sessionDataSet.iterator();
      while (iterator.next()) {
        Assert.assertTrue(
            iterator.getLong("TimePartition") < 0 || iterator.getLong("DataNodeId") >= 2);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void test5() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      SessionDataSet sessionDataSet =
          session.executeQueryStatement(
              "show disk_usage from root.test.** order by TimePartition desc limit 2");
      SessionDataSet.DataIterator iterator = sessionDataSet.iterator();
      long previousTimePartition = Long.MAX_VALUE;
      int count = 0;
      while (iterator.next()) {
        count++;
        long currentTimePartition = iterator.getLong("TimePartition");
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
