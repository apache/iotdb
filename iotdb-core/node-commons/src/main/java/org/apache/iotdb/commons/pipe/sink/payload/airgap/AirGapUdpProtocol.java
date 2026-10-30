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

package org.apache.iotdb.commons.pipe.sink.payload.airgap;

public final class AirGapUdpProtocol {

  // Reserve the request ID's sign bit for requests that must not produce any reverse traffic.
  // The remaining 63 bits identify the request; acknowledged requests keep their existing envelope.
  public static final long NO_ACK_REQUEST_ID_MASK = Long.MIN_VALUE;

  private AirGapUdpProtocol() {}
}
