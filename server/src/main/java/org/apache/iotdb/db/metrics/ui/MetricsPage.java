/**
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
package org.apache.iotdb.db.metrics.ui;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.metrics.server.SqlArgument;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;

import com.codahale.metrics.MetricRegistry;

public class MetricsPage {

	private MetricRegistry mr;
	private List<SqlArgument> list;

	public List<SqlArgument> getList() {
		return list;
	}

	public void setList(List<SqlArgument> list) {
		this.list = list;
	}

	public MetricsPage(MetricRegistry metricRegistry) {
		this.mr = metricRegistry;
	}

	public String render() {
		String html = "";
		String tmpStr = "";
		try {
			String fileName = this.getClass().getClassLoader().getResource("iotdb/ui/static/index.html").getPath();
			FileInputStream is = new FileInputStream(fileName);
			BufferedReader br = new BufferedReader(new InputStreamReader(is));
			while ((tmpStr = br.readLine()) != null) {
				html += tmpStr;
			}
			is.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		html = html.replace("{version}", IoTDBConstant.VERSION);
		html = html.replace("{server}", mr.getGauges().get("iot-metrics.host").getValue() + ":"
				+ mr.getGauges().get("iot-metrics.port").getValue());
		html = html.replace("{cpu}", mr.getGauges().get("iot-metrics.cores").getValue() + " Total, "
				+ mr.getGauges().get("iot-metrics.cpu_ratio").getValue() + "% CPU Ratio");
		html = html.replace("{jvm_mem}",
				mr.getGauges().get("iot-metrics.max_memory").getValue() + "  "
						+ mr.getGauges().get("iot-metrics.total_memory").getValue() + "  "
						+ mr.getGauges().get("iot-metrics.free_memory").getValue() + " (Max/Total/Free)MB");
		html = html.replace("{host_mem}",
				String.format("%.0f",
						((int) mr.getGauges().get("iot-metrics.totalPhysical_memory").getValue() / 1024.0))
						+ " GB Total,  "
						+ String.format("%.1f",
								((int) mr.getGauges().get("iot-metrics.usedPhysical_memory").getValue() / 1024.0))
						+ " GB Used");
		html = html.replace("{sql_table}", sqlRow());
		return html;
	}

	public StringBuilder sqlRow() {
		StringBuilder table = new StringBuilder();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		SqlArgument sqlArgument;
		TSExecuteStatementResp resp;
		String errMsg;
		int statusCode;
		for (int i = (list.size() - 1); i >= 0; i--) {
			sqlArgument = list.get(i);
			resp = sqlArgument.getTSExecuteStatementResp();
			errMsg = resp.getStatus().getStatusType().getMessage();
			statusCode = resp.getStatus().getStatusType().getCode();
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

			table.append("<tr>" + "<td>" + resp.getOperationType() + "</td>" + "<td>"
					+ sdf.format(new Date(sqlArgument.getStartTime())) + "</td>" + "<td>"
					+ sdf.format(new Date(sqlArgument.getEndTime())) + "</td>" + "<td>"
					+ (int) (sqlArgument.getEndTime() - sqlArgument.getStartTime()) + " ms</td>"
					+ "<td class=\"sql\">" + sqlArgument.getStatement() + "</td>" + "<td>" + status + "</td>"
					+ "<td>" + (errMsg.equals("") ? "== Parsed Physical Plan ==" : errMsg)
					+ "<span class=\"expand-details\" onclick=\"this.parentNode.querySelector('.stacktrace-details').classList.toggle('collapsed')\">+ details</span>"
					+ "<div class=\"stacktrace-details collapsed\">" + "<pre>Physical Plan: "
					+ sqlArgument.getPlan().getClass().getSimpleName() + "</br>===========================</br>"
					+ "OperatorType: " + sqlArgument.getPlan().getOperatorType()
					+ "</br>===========================</br>" + "Path: " + sqlArgument.getPlan().getPaths().toString()
					+ "</pre>" + "</div>" + "</td>" + "</tr>");
		}
		return table;
	}

}
