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

package org.apache.iotdb.commons.exception;

import org.apache.iotdb.rpc.TSStatusCode;

public class MetadataLeaseFencedException extends IoTDBRuntimeException {

  public enum LeaseFencedRetryPolicy {
    NONE,
    RETRY_UNTIL_SUCCESS
  }

  public MetadataLeaseFencedException(
      String message, LeaseFencedRetryPolicy leaseFencedRetryPolicy) {
    super(message, getStatusCode(leaseFencedRetryPolicy));
  }

  public MetadataLeaseFencedException(
      Throwable cause, LeaseFencedRetryPolicy leaseFencedRetryPolicy) {
    super(cause, getStatusCode(leaseFencedRetryPolicy));
  }

  private static int getStatusCode(LeaseFencedRetryPolicy leaseFencedRetryPolicy) {
    return leaseFencedRetryPolicy == LeaseFencedRetryPolicy.RETRY_UNTIL_SUCCESS
        ? TSStatusCode.METADATA_LEASE_FENCED_RETRY_REQUIRED.getStatusCode()
        : TSStatusCode.METADATA_LEASE_FENCED.getStatusCode();
  }
}
