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
package org.apache.iotdb.confignode.cq;

import org.apache.iotdb.confignode.manager.cq.CQScheduleTask;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CQScheduleTaskTest {

  @Test
  public void testGetFirstExecutionTime1() {
    long now = 100L;
    long boundaryTime = 0L;
    long everyInterval = 30L;
    assertEquals(120L, CQScheduleTask.getFirstExecutionTime(boundaryTime, everyInterval, now));
  }

  @Test
  public void testGetFirstExecutionTime2() {
    long now = 100L;
    long boundaryTime = 110L;
    long everyInterval = 30L;
    assertEquals(110L, CQScheduleTask.getFirstExecutionTime(boundaryTime, everyInterval, now));
  }
}
