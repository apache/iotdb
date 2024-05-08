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
package org.apache.iotdb.db.protocol.influxdb.util;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.db.utils.DataTypeUtils;
import org.apache.iotdb.protocol.influxdb.rpc.thrift.InfluxCloseSessionReq;
import org.apache.iotdb.protocol.influxdb.rpc.thrift.InfluxOpenSessionReq;
import org.apache.iotdb.protocol.influxdb.rpc.thrift.InfluxOpenSessionResp;
import org.apache.iotdb.service.rpc.thrift.TSCloseSessionReq;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionReq;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionResp;

public class InfluxReqAndRespUtils {

  public static TSOpenSessionReq convertOpenSessionReq(InfluxOpenSessionReq influxOpenSessionReq) {
    TSOpenSessionReq tsOpenSessionReq = new TSOpenSessionReq();
    tsOpenSessionReq.setZoneId(influxOpenSessionReq.getZoneId());
    tsOpenSessionReq.setUsername(influxOpenSessionReq.getUsername());
    tsOpenSessionReq.setPassword(influxOpenSessionReq.getPassword());
    tsOpenSessionReq.setConfiguration(influxOpenSessionReq.getConfiguration());
    return tsOpenSessionReq;
  }

  public static InfluxOpenSessionResp convertOpenSessionResp(TSOpenSessionResp tsOpenSessionResp) {
    InfluxOpenSessionResp influxOpenSessionResp = new InfluxOpenSessionResp();
    influxOpenSessionResp.setSessionId(tsOpenSessionResp.getSessionId());
    TSStatus tsStatus = tsOpenSessionResp.getStatus();
    influxOpenSessionResp.setStatus(DataTypeUtils.RPCStatusToInfluxDBTSStatus(tsStatus));
    return influxOpenSessionResp;
  }

  public static TSCloseSessionReq convertCloseSessionReq(
      InfluxCloseSessionReq influxCloseSessionReq) {
    TSCloseSessionReq tsCloseSessionReq = new TSCloseSessionReq();
    tsCloseSessionReq.setSessionId(influxCloseSessionReq.getSessionId());
    return tsCloseSessionReq;
  }
}
