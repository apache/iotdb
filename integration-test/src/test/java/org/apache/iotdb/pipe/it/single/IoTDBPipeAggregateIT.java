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

package org.apache.iotdb.pipe.it.single;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT1;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT1.class})
public class IoTDBPipeAggregateIT extends AbstractPipeSingleIT {
  @Test
  @Ignore
  public void testAggregator() throws Exception {
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) env.getLeaderConfigNodeConnection()) {
      // Test the mixture of historical and realtime data
      // Do not fail if the failure has nothing to do with pipe
      // Because the failures will randomly generate due to resource limitation
      TestUtils.executeNonQueries(
          env,
          Arrays.asList(
              "create timeseries root.ln.wf01.wt01.temperature with datatype=FLOAT, encoding=RLE, compression=SNAPPY tags(tag1=v1, tag2=v2) attributes(attr1=v1, attr2=v2)",
              "create timeseries root.ln.wf01.wt01.status with datatype=BOOLEAN, encoding=RLE, compression=SNAPPY",
              "insert into root.ln.wf01.wt01(time, temperature, status) values (10000, 1, false)"),
          null);

      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("pattern", "root.ln");

      processorAttributes.put("processor", "aggregate-processor");
      processorAttributes.put("output.database", "root.testdb");
      processorAttributes.put(
          "output.measurements", "Avg1, peak1, rms1, var1, skew1, kurt1, ff1, cf1, pf1");
      processorAttributes.put(
          "operators", "avg, peak, rms, var, skew, kurt, ff, cf, pf, cE, max, min");
      processorAttributes.put("sliding.seconds", "60");

      sinkAttributes.put("sink", "write-back-sink");

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("testPipe", sinkAttributes)
                  .setExtractorAttributes(sourceAttributes)
                  .setProcessorAttributes(processorAttributes));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      // Test unsupported types
      TestUtils.executeNonQueries(
          env,
          Arrays.asList(
              "create timeSeries root.ln.wf01.wt01.boolean boolean",
              "create timeSeries root.ln.wf01.wt01.date date",
              "create timeSeries root.ln.wf01.wt01.text text",
              "create timeSeries root.ln.wf01.wt01.string string",
              "create timeSeries root.ln.wf01.wt01.blob blob",
              "insert into root.ln.wf01.wt01(time, boolean, date, text, string, blob) values (20000, false, '2000-12-13', 'abc', 'def', X'f103')",
              "flush"),
          null);

      TestUtils.executeNonQueries(
          env,
          Arrays.asList(
              "insert into root.ln.wf01.wt01(time, temperature, status) values (20000, 2, true)",
              "insert into root.ln.wf01.wt01(time, temperature, status) values (30000, 3, false)",
              "insert into root.ln.wf01.wt01(time, temperature, status) values (40000, 4, true)",
              "insert into root.ln.wf01.wt01(time, temperature, status) values (50000, 5, false)",
              "insert into root.ln.wf01.wt01(time, temperature, status) values (60000, 6, true)",
              "insert into root.ln.wf01.wt01(time, temperature, status) values (70000, 7, false)",
              "insert into root.ln.wf01.wt01(time, temperature, status) values (80000, 8, true)",
              "insert into root.ln.wf01.wt01(time, temperature, status) values (90000, 9, false)",
              "insert into root.ln.wf01.wt01(time, temperature, status) values (100000, 10, true)",
              "insert into root.ln.wf01.wt01(time, temperature, status) values (110000, 11, false)",
              "insert into root.ln.wf01.wt01(time, temperature, status) values (120000, 12, false)",
              "flush"),
          null);

      // Test total number
      TestUtils.assertDataEventuallyOnEnv(
          env,
          "select count(*) from root.testdb.** group by level=1",
          "count(root.testdb.*.*.*.*),",
          Collections.singleton("24,"));

      // Test manually renamed timeSeries count
      TestUtils.assertDataEventuallyOnEnv(
          env,
          "select count(Avg1) from root.testdb.wf01.wt01.temperature",
          "count(root.testdb.wf01.wt01.temperature.Avg1),",
          Collections.singleton("2,"));

      // Test default renamed timeSeries count
      TestUtils.assertDataEventuallyOnEnv(
          env,
          "select count(cE) from root.testdb.wf01.wt01.temperature",
          "count(root.testdb.wf01.wt01.temperature.cE),",
          Collections.singleton("2,"));
    }
  }
}
