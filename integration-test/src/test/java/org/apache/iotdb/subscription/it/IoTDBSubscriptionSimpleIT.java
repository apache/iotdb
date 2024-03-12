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

package org.apache.iotdb.subscription.it;

import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.rpc.subscription.payload.request.ConsumerConfig;
import org.apache.iotdb.rpc.subscription.payload.response.EnrichedTablets;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class IoTDBSubscriptionSimpleIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBSubscriptionSimpleIT.class);

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void basicSubscriptionTest() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      // prepare data
      List<MeasurementSchema> schemaList = new ArrayList<>();
      schemaList.add(new MeasurementSchema("s1", TSDataType.INT64));
      schemaList.add(new MeasurementSchema("s2", TSDataType.DOUBLE));
      schemaList.add(new MeasurementSchema("s3", TSDataType.TEXT));

      Tablet tablet = new Tablet("root.sg.d", schemaList, 10);

      long timestamp = System.currentTimeMillis();

      for (long row = 0; row < 15; row++) {
        int rowIndex = tablet.rowSize++;
        tablet.addTimestamp(rowIndex, timestamp);
        tablet.addValue("s1", rowIndex, 1L);
        tablet.addValue("s2", rowIndex, 1D);
        tablet.addValue("s3", rowIndex, new Binary("1", TSFileConfig.STRING_CHARSET));
        if (tablet.rowSize == tablet.getMaxRowNumber()) {
          session.insertTablet(tablet, true);
          tablet.reset();
        }
        timestamp++;
      }

      if (tablet.rowSize != 0) {
        session.insertTablet(tablet);
        tablet.reset();
      }

      // subscription
      session.createConsumer(new ConsumerConfig("cg1", "c1"));
      session.subscribe(Collections.singletonList("topic1"));
      // TODO: manually create pipe
      session.executeNonQueryStatement(
          "create pipe topic1_cg1 with source ('source'='iotdb-source', 'inclusion'='data', 'inclusion.exclusion'='deletion') with sink ('sink'='subscription-sink', 'topic'='topic1', 'consumer-group'='cg1')");
      Thread.sleep(2333);
      List<EnrichedTablets> enrichedTabletsList = session.poll(Collections.singletonList("topic1"));
      Map<String, List<String>> topicNameToSubscriptionCommitIds = new HashMap<>();
      for (EnrichedTablets enrichedTablets : enrichedTabletsList) {
        System.out.println(enrichedTablets.getTopicName());
        System.out.println(enrichedTablets.getTablets());
        System.out.println(enrichedTablets.getSubscriptionCommitIds());
        topicNameToSubscriptionCommitIds
            .computeIfAbsent(enrichedTablets.getTopicName(), (topicName) -> new ArrayList<>())
            .addAll(enrichedTablets.getSubscriptionCommitIds());
      }
      session.commit(topicNameToSubscriptionCommitIds);
      session.unsubscribe(Collections.singletonList("topic1"));
      session.dropConsumer();
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }
}
