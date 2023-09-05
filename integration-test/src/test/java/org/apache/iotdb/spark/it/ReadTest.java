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
package org.apache.iotdb.spark.it;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.spark.db.Transformer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ReadTest extends AbstractTest {

  @Before
  @Override
  public void before() throws ClassNotFoundException, IoTDBConnectionException {
    super.before();
    Utils.prepareData(jdbcUrl);
  }

  @After
  @Override
  public void after() throws IoTDBConnectionException {
    super.after();
  }

  @Test
  public void testShowData() {
    Dataset<Row> df =
        spark
            .read()
            .format("org.apache.iotdb.spark.db")
            .option("url", jdbcUrl)
            .option("sql", "select ** from root")
            .load();
    Assert.assertEquals(7505, df.count());
  }

  @Test
  public void testShowDataWithPartition() {
    Dataset<Row> df =
        spark
            .read()
            .format("org.apache.iotdb.spark.db")
            .option("url", jdbcUrl)
            .option("sql", "select ** from root")
            .option("lowerBound", 1)
            .option("upperBound", System.nanoTime() / 1000 / 1000)
            .option("numPartition", 10)
            .load();

    Assert.assertEquals(7505, df.count());
  }

  @Test
  public void testFilterData() {
    Dataset<Row> df =
        spark
            .read()
            .format("org.apache.iotdb.spark.db")
            .option("url", jdbcUrl)
            .option("sql", "select ** from root where time < 2000 and time > 1000")
            .load();

    Assert.assertEquals(499, df.count());
  }

  @Test
  public void testFilterDataWithPartition() {
    Dataset<Row> df =
        spark
            .read()
            .format("org.apache.iotdb.spark.db")
            .option("url", jdbcUrl)
            .option("sql", "select ** from root where time < 2000 and time > 1000")
            .option("lowerBound", 1)
            .option("upperBound", 10000)
            .option("numPartition", 10)
            .load();

    Assert.assertEquals(499, df.count());
  }

  @Test
  public void testTransformToNarrow() {
    Dataset<Row> df =
        spark
            .read()
            .format("org.apache.iotdb.spark.db")
            .option("url", jdbcUrl)
            .option("sql", "select ** from root where time < 1100 and time > 1000")
            .load();

    Dataset<Row> narrowDf = Transformer.toNarrowForm(spark, df);
    Assert.assertEquals(198, narrowDf.count());
  }

  @Test
  public void testTransformBackToWide() {
    Dataset<Row> df =
        spark
            .read()
            .format("org.apache.iotdb.spark.db")
            .option("url", jdbcUrl)
            .option("sql", "select ** from root where time < 1100 and time > 1000")
            .load();
    Dataset<Row> narrowDf = Transformer.toNarrowForm(spark, df);
    Dataset<Row> wideDf = Transformer.toWideForm(spark, narrowDf);
    Assert.assertEquals(99, wideDf.count());
  }

  @Test
  public void testAggregateSql() {
    Dataset<Row> df =
        spark
            .read()
            .format("org.apache.iotdb.spark.db")
            .option("url", jdbcUrl)
            .option("sql", "select count(d0.s0),count(d0.s1) from root.vehicle")
            .load();

    Row row = df.collectAsList().get(0);
    Assert.assertEquals("7500", row.get(0).toString());
    Assert.assertEquals("7500", row.get(1).toString());
  }
}
