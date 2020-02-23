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

import com.alibaba.fastjson.JSONArray;
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

  private static final String HOST = "host";
  private static final String PORT = "port";
  private static final String CORES = "cores";
  private static final String CPU_RATIO = "cpu_ratio";
  private static final String TOTAL_MEMORY = "total_memory";
  private static final String MAX_MEMORY = "max_memory";
  private static final String FREE_MEMORY = "free_memory";
  private static final String TOTAL_PHYSICAL_MEMORY = "totalPhysical_memory";
  private static final String FREE_PHYSICAL_MEMORY = "freePhysical_memory";
  private static final String USED_PHYSICAL_MEMORY = "usedPhysical_memory";
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

  private SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss:SSS");
  private ServerArgument serverArgument = new ServerArgument(IoTDBDescriptor.getInstance().getConfig().getRestPort());

  public MetricsSystem() {
    String sourceName = "iot-metrics";
    metricRegistry.register(MetricRegistry.name(sourceName, HOST),
        (Gauge<String>) serverArgument::getHost);
    
    metricRegistry.register(MetricRegistry.name(sourceName, PORT),
        (Gauge<Integer>) serverArgument::getPort);
    
    metricRegistry.register(MetricRegistry.name(sourceName, CORES),
        (Gauge<Integer>) serverArgument::getCores);
    
    metricRegistry.register(MetricRegistry.name(sourceName, CPU_RATIO),
        (Gauge<Integer>) serverArgument::getCpuRatio);

    metricRegistry.register(MetricRegistry.name(sourceName, TOTAL_MEMORY),
        (Gauge<Long>) serverArgument::getTotalMemory);

    metricRegistry.register(MetricRegistry.name(sourceName, MAX_MEMORY),
        (Gauge<Long>) serverArgument::getMaxMemory);

    metricRegistry.register(MetricRegistry.name(sourceName, FREE_MEMORY),
        (Gauge<Long>) serverArgument::getFreeMemory);

    metricRegistry.register(MetricRegistry.name(sourceName, TOTAL_PHYSICAL_MEMORY),
        (Gauge<Long>) serverArgument::getTotalPhysicalMemory);

    metricRegistry.register(MetricRegistry.name(sourceName, FREE_PHYSICAL_MEMORY),
        (Gauge<Long>) serverArgument::getFreePhysicalMemory);

    metricRegistry.register(MetricRegistry.name(sourceName, USED_PHYSICAL_MEMORY),
        (Gauge<Long>) serverArgument::getUsedPhysicalMemory);
  }

  public JSONObject metrics_json() throws JsonProcessingException {
    return (JSONObject) JSONObject.parse(om.writeValueAsString(metricRegistry));
  }

  public JSONObject server_json() {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put(HOST, serverArgument.getHost());
    jsonObject.put(PORT, serverArgument.getPort());
    jsonObject.put(CORES, serverArgument.getCores());
    jsonObject.put(CPU_RATIO, serverArgument.getCpuRatio());
    jsonObject.put(TOTAL_MEMORY, serverArgument.getTotalMemory());
    jsonObject.put(MAX_MEMORY, serverArgument.getMaxMemory());
    jsonObject.put(FREE_MEMORY, serverArgument.getFreeMemory());
    jsonObject.put(TOTAL_PHYSICAL_MEMORY, String.format("%.0f",serverArgument.getTotalPhysicalMemory() / 1024.0));
    jsonObject.put(FREE_PHYSICAL_MEMORY, String.format("%.0f",serverArgument.getFreePhysicalMemory() / 1024.0));
    jsonObject.put(USED_PHYSICAL_MEMORY, String.format("%.0f",serverArgument.getUsedPhysicalMemory() / 1024.0));
    return jsonObject;
  }

  public JSONArray sql_json() {
    JSONArray jsonArray = new JSONArray();
    for (SqlArgument sqlArgument : sqlArgumentsList) {
      JSONObject sql = new JSONObject();
      TSExecuteStatementResp resp = sqlArgument.getTSExecuteStatementResp();
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
      sql.put("startTime", sdf.format(new Date(sqlArgument.getStartTime())));
      sql.put("endTime", sdf.format(new Date(sqlArgument.getEndTime())));
      sql.put("time", sqlArgument.getEndTime() - sqlArgument.getStartTime());
      sql.put("sql", sqlArgument.getStatement());
      sql.put("status", status);
      sql.put("errMsg", errMsg);
      sql.put("physicalPlan", sqlArgument.getPlan().getClass().getSimpleName());
      sql.put("operatorType", sqlArgument.getPlan().getOperatorType());
      sql.put("path", sqlArgument.getPlan().getPaths().toString());
      jsonArray.add(sql);
    }
    return jsonArray;
  }

}
