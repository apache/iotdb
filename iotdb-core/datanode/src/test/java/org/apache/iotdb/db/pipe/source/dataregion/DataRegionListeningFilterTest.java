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

package org.apache.iotdb.db.pipe.source.dataregion;

import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.commons.subscription.meta.topic.TopicMeta;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DataRegionListeningFilterTest {

  @Test
  public void testAuditDatabaseIsNeverListened() throws Exception {
    final Map<String, String> topicAttributes = new HashMap<>();
    topicAttributes.put(SystemConstant.SQL_DIALECT_KEY, SystemConstant.SQL_DIALECT_TABLE_VALUE);
    final PipeParameters parameters =
        new PipeParameters(
            new TopicMeta("topic", 1, topicAttributes).generateExtractorAttributes("root"));

    assertFalse(DataRegionListeningFilter.shouldDatabaseBeListened(parameters, true, "__audit"));
    assertFalse(
        DataRegionListeningFilter.shouldDatabaseBeListened(parameters, false, "root.__audit"));
    assertFalse(DataRegionListeningFilter.shouldDatabaseBeListened(parameters, true, "__AUDIT"));
    assertTrue(DataRegionListeningFilter.shouldDatabaseBeListened(parameters, true, "user_db"));
  }
}
