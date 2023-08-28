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
package org.apache.iotdb.web.grafana.dao.impl;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.test.util.ReflectionTestUtils;

public class BasicDaoImplTest {

  @Before
  public void setUp() {}

  @After
  public void tearDown() {}

  @Test
  public void getInterval() {
    BasicDaoImpl impl = new BasicDaoImpl(null);
    ReflectionTestUtils.setField(impl, "isDownSampling", true);
    ReflectionTestUtils.setField(impl, "interval", "1m");

    String interval1 = impl.getInterval(0);
    Assert.assertEquals("", interval1);

    String interval2 = impl.getInterval(3);
    Assert.assertEquals("1m", interval2);

    String interval3 = impl.getInterval(25);
    Assert.assertEquals("1h", interval3);

    String interval4 = impl.getInterval(24 * 30 + 1);
    Assert.assertEquals("1d", interval4);
  }
}
