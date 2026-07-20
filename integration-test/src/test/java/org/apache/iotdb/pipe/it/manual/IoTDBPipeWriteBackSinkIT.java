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

package org.apache.iotdb.pipe.it.manual;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2ManualCreateSchema;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2ManualCreateSchema.class})
public class IoTDBPipeWriteBackSinkIT extends AbstractPipeDualManualIT {

  @Test
  public void testWriteBackSinkWithTargetDatabaseForTreeModel() throws Exception {
    TestUtils.executeNonQueries(
        senderEnv,
        Arrays.asList(
            "create database root.source",
            "create timeseries root.source.d1.s1 with datatype=INT32,encoding=PLAIN",
            "create database root.target.db",
            "create timeseries root.target.db.d1.s1 with datatype=INT32,encoding=PLAIN"),
        null);

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("extractor.inclusion", "data.insert");
      sourceAttributes.put("extractor.forwarding-pipe-requests", "false");
      sourceAttributes.put("extractor.path", "root.source.**");
      sourceAttributes.put("user", "root");

      sinkAttributes.put("sink", "write-back-sink");
      sinkAttributes.put("sink.database", "root.target.db");
      sinkAttributes.put("user", "root");

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("testPipe", sinkAttributes)
                  .setExtractorAttributes(sourceAttributes)
                  .setProcessorAttributes(processorAttributes));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("testPipe").getCode());
    }

    TestUtils.executeNonQueries(
        senderEnv,
        Arrays.asList(
            "insert into root.source.d1(time, s1) values (1, 1)",
            "insert into root.source.d1(time, s1) values (2, 2)",
            "flush"),
        null);

    TestUtils.assertDataEventuallyOnEnv(
        senderEnv,
        "select * from root.target.db.**",
        "Time,root.target.db.d1.s1,",
        Collections.unmodifiableSet(new HashSet<>(Arrays.asList("1,1,", "2,2,"))));
  }
}
