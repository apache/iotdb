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

package org.apache.iotdb.rpc;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;

/** The UrlUtils */
public class UrlUtils {
  private static final String POINT_COLON = ":";

  private UrlUtils() {}

  /**
   * Parse TEndPoint from a given TEndPointUrl example:D80:0000:0000:0000:ABAA:0000:00C2:0002:22227
   *
   * @param endPointUrl ip:port
   * @return TEndPoint null if parse error
   */
  public static TEndPoint parseTEndPointIpv4AndIpv6Url(String endPointUrl) {
    TEndPoint endPoint = new TEndPoint();
    if (endPointUrl.contains(POINT_COLON)) {
      int i = endPointUrl.lastIndexOf(POINT_COLON);
      String port = endPointUrl.substring(endPointUrl.lastIndexOf(POINT_COLON) + 1);
      String ip = endPointUrl.substring(0, i);
      endPoint.setIp(ip);
      endPoint.setPort(Integer.parseInt(port));
    }
    return endPoint;
  }
}
