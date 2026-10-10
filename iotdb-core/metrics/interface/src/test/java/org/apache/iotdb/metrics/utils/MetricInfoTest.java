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

package org.apache.iotdb.metrics.utils;

import org.junit.Assert;
import org.junit.Test;

public class MetricInfoTest {

  @Test
  public void metaInfoEqualsHashCodeUsesTypeAndSameTagKeys() {
    MetricInfo.MetaInfo metaInfo =
        new MetricInfo(MetricType.COUNTER, "metric", "k1", "v1").getMetaInfo();
    MetricInfo.MetaInfo sameMetaInfo =
        new MetricInfo(MetricType.COUNTER, "metric", "k1", "v2").getMetaInfo();
    MetricInfo.MetaInfo extraTagMetaInfo =
        new MetricInfo(MetricType.COUNTER, "metric", "k1", "v1", "k2", "v2").getMetaInfo();
    MetricInfo.MetaInfo differentTypeMetaInfo =
        new MetricInfo(MetricType.GAUGE, "metric", "k1", "v1").getMetaInfo();

    Assert.assertEquals(metaInfo, sameMetaInfo);
    Assert.assertEquals(metaInfo.hashCode(), sameMetaInfo.hashCode());
    Assert.assertNotEquals(metaInfo, extraTagMetaInfo);
    Assert.assertNotEquals(extraTagMetaInfo, metaInfo);
    Assert.assertNotEquals(metaInfo, differentTypeMetaInfo);
  }
}
