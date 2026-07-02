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

package org.apache.iotdb.commons.utils;

import org.junit.Assert;
import org.junit.Test;

public class LogThrottlerTest {

  @Test
  public void sameFailureLoggedOnlyAfterIntervalOrReset() {
    LogThrottler logThrottler = new LogThrottler();
    long startTime = 1000L;

    Assert.assertTrue(logThrottler.shouldLog("failure", startTime));
    Assert.assertFalse(logThrottler.shouldLog("failure", startTime + 1));

    Assert.assertTrue(
        logThrottler.shouldLog("failure", startTime + LogThrottler.DEFAULT_LOG_INTERVAL_MS));
    Assert.assertFalse(
        logThrottler.shouldLog("failure", startTime + LogThrottler.DEFAULT_LOG_INTERVAL_MS + 1));

    logThrottler.reset();
    Assert.assertTrue(logThrottler.shouldLog("failure", startTime + 2));
  }

  @Test
  public void differentFailuresAndKeysAreLoggedIndependently() {
    LogThrottler logThrottler = new LogThrottler();
    long startTime = 1000L;

    Assert.assertTrue(logThrottler.shouldLog("key1", "failure1", startTime));
    Assert.assertFalse(logThrottler.shouldLog("key1", "failure1", startTime + 1));
    Assert.assertTrue(logThrottler.shouldLog("key1", "failure2", startTime + 2));

    Assert.assertTrue(logThrottler.shouldLog("key2", "failure1", startTime + 3));
    Assert.assertFalse(logThrottler.shouldLog("key2", "failure1", startTime + 4));

    logThrottler.reset("key2");
    Assert.assertFalse(logThrottler.shouldLog("key1", "failure2", startTime + 5));
    Assert.assertTrue(logThrottler.shouldLog("key2", "failure1", startTime + 6));
  }

  @Test
  public void exceptionSignatureIncludesCause() {
    RuntimeException failure = new RuntimeException("outer", new IllegalStateException("inner"));

    Assert.assertEquals(
        "java.lang.RuntimeException:outer;cause=java.lang.IllegalStateException:inner",
        LogThrottler.getFailureSignature(failure));
  }

  @Test(expected = IllegalArgumentException.class)
  public void negativeIntervalRejected() {
    new LogThrottler(-1);
  }
}
