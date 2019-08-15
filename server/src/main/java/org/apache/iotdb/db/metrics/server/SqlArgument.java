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
package org.apache.iotdb.db.metrics.server;

import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;

public class SqlArgument {

	TSExecuteStatementResp TSExecuteStatementResp;
	PhysicalPlan plan;
	String statement;
	long starttime;
	long endtime;

	public SqlArgument(org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp tSExecuteStatementResp,
			PhysicalPlan plan, String statement, long starttime, long endtime) {
		this.TSExecuteStatementResp = tSExecuteStatementResp;
		this.starttime = starttime;
		this.endtime = endtime;
		this.plan = plan;
	}

	public TSExecuteStatementResp getTSExecuteStatementResp() {
		return TSExecuteStatementResp;
	}

	public void setTSExecuteStatementResp(TSExecuteStatementResp tSExecuteStatementResp) {
		TSExecuteStatementResp = tSExecuteStatementResp;
	}

	public long getStarttime() {
		return starttime;
	}

	public void setStarttime(long starttime) {
		this.starttime = starttime;
	}

	public long getEndtime() {
		return endtime;
	}

	public void setEndtime(long endtime) {
		this.endtime = endtime;
	}

	public PhysicalPlan getPlan() {
		return plan;
	}

	public void setPlan(PhysicalPlan plan) {
		this.plan = plan;
	}

	public String getStatement() {
		return statement;
	}

	public void setStatement(String statement) {
		this.statement = statement;
	}

}
