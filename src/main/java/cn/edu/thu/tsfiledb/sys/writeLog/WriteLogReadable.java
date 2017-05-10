package cn.edu.thu.tsfiledb.sys.writeLog;

import java.io.IOException;

import cn.edu.thu.tsfiledb.qp.physical.plan.PhysicalPlan;

public interface WriteLogReadable {
	
	/**
	 * check whether there is an operator in the source.
	 * Note: check process starts from the end of the source.
	 */
	public boolean hasNextOperator() throws IOException;
	
	public byte[] nextOperator() throws IOException;

	public PhysicalPlan getPhysicalPlan() throws IOException;
}
