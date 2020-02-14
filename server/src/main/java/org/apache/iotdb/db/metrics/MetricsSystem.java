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
package org.apache.iotdb.db.metrics;

import com.alibaba.fastjson.JSONObject;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.json.MetricsModule;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.service.TSServiceImpl;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;

public class MetricsSystem {

  /**
   * om is used for writing metricRegistry
   */
  private ObjectMapper om = new ObjectMapper()
      .registerModule(new MetricsModule(TimeUnit.SECONDS, TimeUnit.MILLISECONDS, false));

  /**
   * metricRegistry is used fro registering server information
   */
  private MetricRegistry metricRegistry = new MetricRegistry();
  private List<SqlArgument> sqlArgumentsList = TSServiceImpl.sqlArgumentsList;

  /**
   * start index of unused sqlArgument in sqlArgumentsList
   */
  private int start = 0;

  private SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss:SSS");

  public MetricsSystem() {
    ServerArgument serverArgument = new ServerArgument(IoTDBDescriptor.getInstance().getConfig().getRestPort());
    String sourceName = "iot-metrics";
    metricRegistry.register(MetricRegistry.name(sourceName, "host"),
        (Gauge<String>) serverArgument::getHost);
    metricRegistry.register(MetricRegistry.name(sourceName, "port"),
        (Gauge<Integer>) serverArgument::getPort);

    metricRegistry.register(MetricRegistry.name(sourceName, "cores"),
        (Gauge<Integer>) serverArgument::getCores);

    metricRegistry.register(MetricRegistry.name(sourceName, "cpu_ratio"),
        (Gauge<Integer>) serverArgument::getCpuRatio);

    metricRegistry.register(MetricRegistry.name(sourceName, "total_memory"),
        (Gauge<Long>) serverArgument::getTotalMemory);

    metricRegistry.register(MetricRegistry.name(sourceName, "max_memory"),
        (Gauge<Long>) serverArgument::getMaxMemory);

    metricRegistry.register(MetricRegistry.name(sourceName, "free_memory"),
        (Gauge<Long>) serverArgument::getFreeMemory);

    metricRegistry.register(MetricRegistry.name(sourceName, "totalPhysical_memory"),
        (Gauge<Long>) serverArgument::getTotalPhysicalMemory);

    metricRegistry.register(MetricRegistry.name(sourceName, "freePhysical_memory"),
        (Gauge<Long>) serverArgument::getFreePhysicalMemory);

    metricRegistry.register(MetricRegistry.name(sourceName, "usedPhysical_memory"),
        (Gauge<Long>) serverArgument::getUsedPhysicalMemory);
  }

  public JSONObject json() throws JsonProcessingException {
    return (JSONObject) JSONObject.parse(om.writeValueAsString(metricRegistry));
  }

  public JSONObject json_sql() {
    JSONObject sql = new JSONObject();
    for(int i = start; i < sqlArgumentsList.size(); i++) {
      start++;
      TSExecuteStatementResp resp = sqlArgumentsList.get(i).getTSExecuteStatementResp();
      String errMsg = resp.getStatus().getStatusType().getMessage();
      int statusCode = resp.getStatus().getStatusType().getCode();
      String status;
      if (statusCode == 200) {
        status = "FINISHED";
      } else if (statusCode == 201) {
        status = "EXECUTING";
      } else if (statusCode == 202) {
        status = "INVALID_HANDLE";
      } else {
        status = "FAILED";
      }
      sql.put("startTime", sdf.format(new Date(sqlArgumentsList.get(i).getStartTime())));
      sql.put("endTime", sdf.format(new Date(sqlArgumentsList.get(i).getEndTime())));
      sql.put("time", sqlArgumentsList.get(i).getEndTime() - sqlArgumentsList.get(i).getStartTime());
      sql.put("sql", sqlArgumentsList.get(i).getStatement());
      sql.put("status", status);
      sql.put("errMsg", errMsg);
      sql.put("physicalPlan", sqlArgumentsList.get(i).getPlan().getClass().getSimpleName());
      sql.put("operatorType", sqlArgumentsList.get(i).getPlan().getOperatorType());
      sql.put("path", sqlArgumentsList.get(i).getPlan().getPaths().toString());
    }
    return sql;
  }

}
