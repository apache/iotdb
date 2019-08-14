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
