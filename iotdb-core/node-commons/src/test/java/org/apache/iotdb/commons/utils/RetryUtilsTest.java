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

import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Test;

public class RetryUtilsTest {

  /**
   * Verifies that transient write failures, including WRITE_PROCESS_ERROR returned for an
   * IOException during SyncLog, are retried while successful writes are not.
   */
  @Test
  public void testNeedRetryForWrite() {
    Assert.assertTrue(
        RetryUtils.needRetryForWrite(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode()));
    Assert.assertTrue(RetryUtils.needRetryForWrite(TSStatusCode.SYSTEM_READ_ONLY.getStatusCode()));
    Assert.assertTrue(
        RetryUtils.needRetryForWrite(TSStatusCode.WRITE_PROCESS_REJECT.getStatusCode()));
    Assert.assertTrue(
        RetryUtils.needRetryForWrite(TSStatusCode.WRITE_PROCESS_ERROR.getStatusCode()));
    Assert.assertTrue(
        RetryUtils.needRetryForWrite(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode()));
    Assert.assertFalse(RetryUtils.needRetryForWrite(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
  }
}
