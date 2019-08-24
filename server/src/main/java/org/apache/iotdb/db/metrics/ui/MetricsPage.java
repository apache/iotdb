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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.metrics.server.SqlArgument;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;

import com.codahale.metrics.MetricRegistry;

public class MetricsPage {

	private static final String[] appHeaders = {"Operation Type","Start Time","Finish Time","Duration","Statement","State","Detail"};
	private MetricRegistry mr;
	public List<SqlArgument> list;

	public List<SqlArgument> getList() {
		return list;
	}

	public void setList(List<SqlArgument> list) {
		this.list = list;
	}

	public MetricsPage(MetricRegistry metricRegistry) {
		this.mr = metricRegistry;
	}

	public StringBuilder render() {
		StringBuilder html = new StringBuilder();
		html.append("<html><head>");
		html.append(htmlHead());
		html.append("<title>IotDB Server</title>");
		html.append("</head><body>");
		html.append(
				   "<div class=\"navbar navbar-static-top\">"
		         +   "<div class=\"navbar-inner\">"
		         + 	   "<div class=\"brand\">"
		         +      "<h3 style=\"vertical-align: middle; display: inline-block;\">"
		         +   	  "<a href=\"\" style=\"text-decoration: none\">"
		         +      	"<img src=\"/static/iotdb-logo.png\" />"
		         +      	"<span class=\"version\" style=\"margin-right: 15px;\">   "+IoTDBConstant.VERSION+"</span>"
		         +   	  "</a>IOTDB Metrics Server"
		         +       "</h3>"
		         + 	    "</div>"
		         +    "</div>"
		         +   "</div>"
		         +   "<div class=\"container-fluid\">"
		         +   sysInfo()
		         +   "</div>"
		         + "</body>"
		         +"</html>"
				);
		return html;
	}

	public StringBuilder htmlHead() {
		StringBuilder head = new StringBuilder();
		head.append("<meta http-equiv=\"Content-type\" content=\"text/html; charset=utf-8\" />");
		head.append("<link rel=\"stylesheet\" href=\"/static/bootstrap.min.css\" type=\"text/css\"/>");
		head.append("<link rel=\"stylesheet\" href=\"/static/webui.css\" type=\"text/css\"/>");
		return head;
	}

	private StringBuilder sysInfo() {
		StringBuilder info = new StringBuilder();
		info.append(
            "<div class=\"row-fluid\">"
	     +    "<div class=\"span12\">"
	     +     "<ul class=\"unstyled\">"
	     +       "<li><strong>Server &ensp; URL: </strong>"+ mr.getGauges().get("iot-metrics.host").getValue()+ ":"
	                                                       + mr.getGauges().get("iot-metrics.port").getValue()+"</li>"
	     +       "<li><strong>CPU &nbsp;&ensp; Cores: </strong>" + mr.getGauges().get("iot-metrics.cores").getValue()+" Total, "
	                                                       + mr.getGauges().get("iot-metrics.cpu_ratio").getValue()+"% CPU Ratio</li>"
	     +       "<li><strong>JVM  Memory: </strong>"      + mr.getGauges().get("iot-metrics.max_memory").getValue()+"  "
	     											       + mr.getGauges().get("iot-metrics.total_memory").getValue()+"  "
	     											       + mr.getGauges().get("iot-metrics.free_memory").getValue()+" (Max/Total/Free)MB</li>"
	     +       "<li><strong>Host Memory: </strong>"+String.format("%.0f",
	    		                                          ((int)mr.getGauges().get("iot-metrics.totalPhysical_memory").getValue()/1024.0))+" GB Total,  "
	     											 +String.format("%.1f",
	     												  ((int)mr.getGauges().get("iot-metrics.usedPhysical_memory").getValue()/1024.0))+" GB Used</li>"
	     +       "<li><strong>Status: </strong>ALIVE</li>"
	     +     "</ul>"
	     +    "</div>"
	     +   "</div>"
		 +   "<div class=\"row-fluid\">"
		 +      "<div class=\"span12\">"
		 +        "<span class=\"collapse-aggregated-workers collapse-table\" onclick=\"collapseTable('collapse-aggregated-workers','aggregated-workers')\">"
		 +          "<h4>"
		 +            "<a>Excute Sql</a>"
		 + 		    "</h4>"
		 +        "</span>"
		 +        "<div class=\"aggregated-workers collapsible-table\">"
		 +         listingTable(appHeaders)
		 +        "</div>"
		 +      "</div>"
		 +   "</div>"	 
	 );
		return info;
	}

	private StringBuilder appRow(SqlArgument queryResult) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		StringBuilder app = new StringBuilder();
		TSExecuteStatementResp resp = queryResult.getTSExecuteStatementResp();
		String errMsg = resp.getStatus().getErrorMessage();
		int statusCode = resp.getStatus().getStatusCode().getValue();
		String status;
		if(statusCode == 1 || statusCode == 0) {
			status = "FINISHED";
		} else if(statusCode == 2) {
			status = "EXECUTING";
		} else {
			status = "FAILED";
		}

		app.append(
			 "<tr>"
		  +    "<td>"+resp.getOperationType()+"</td>"
		  +    "<td>"+sdf.format(new Date(queryResult.getStartTime()))+"</td>"
		  +    "<td>"+sdf.format(new Date(queryResult.getEndTime()))+"</td>"
		  +    "<td>"+(int)(queryResult.getEndTime()-queryResult.getStartTime())+" ms</td>"
		  +    "<td style=\"font-size:13px\">"+queryResult.getStatement()+"</td>"
		  +    "<td>"+status+"</td>"
		  +    "<td>"+(errMsg.equals("")?"== Parsed Physical Plan ==":errMsg)
		  +      "<span class=\"expand-details\" onclick=\"this.parentNode.querySelector('.stacktrace-details').classList.toggle('collapsed')\">+ details</span>"	
		  +      "<div class=\"stacktrace-details collapsed\">"
		  +         "<pre>Physical Plan: "+queryResult.getPlan().getClass().getSimpleName()+"</br>===========================</br>"
		  +               "OperatorType: "+queryResult.getPlan().getOperatorType()+"</br>===========================</br>"
		  +	              "Path: "+queryResult.getPlan().getPaths().toString()
		  +         "</pre>"
		  +      "</div>"
		  +    "</td>"
		  +  "</tr>"
	 );
		return app;
	}

	public StringBuilder listingTable(String[] headers) {
		StringBuilder table = new StringBuilder();
		table.append("<table class=\"table table-bordered table-condensed table-striped sortable\"><thead><tr>");
		for(String s:headers) {
			table.append("<th width=\"\">"+s+"</th>");
		}
		table.append("</tr></thead><tbody>");
		for(int i=(list.size()-1);i>=0;i--) {
			table.append(appRow(list.get(i)));
		}
		table.append("</tbody><tfoot></tfoot></table>");  
		return table;
	}
}
