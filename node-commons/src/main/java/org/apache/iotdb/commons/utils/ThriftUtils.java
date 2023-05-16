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

import java.util.Objects;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;

/**
 * Utils that extend thrift generated objects.
 */
public class ThriftUtils {
  private ThriftUtils() {
    // Empty constructor
  }

  public static boolean endPointInLocation(TDataNodeLocation location, TEndPoint endPoint) {
    if (Objects.equals(location.getClientRpcEndPoint(), endPoint)) {
      return true;
    }
    if (Objects.equals(location.getDataRegionConsensusEndPoint(), endPoint)) {
      return true;
    }
    if (Objects.equals(location.getSchemaRegionConsensusEndPoint(), endPoint)) {
      return true;
    }
    if (Objects.equals(location.getMPPDataExchangeEndPoint(), endPoint)) {
      return true;
    }
    if (Objects.equals(location.getInternalEndPoint(), endPoint)) {
      return true;
    }
    return false;
  }
}
