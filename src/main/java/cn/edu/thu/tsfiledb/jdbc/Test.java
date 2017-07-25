package cn.edu.thu.tsfiledb.jdbc;

import java.nio.ByteBuffer;
import java.util.List;

import cn.edu.thu.tsfile.common.exception.ProcessorException;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfiledb.conf.TsfileDBDescriptor;
import cn.edu.thu.tsfiledb.exception.ArgsErrorException;
import cn.edu.thu.tsfiledb.qp.QueryProcessor;
import cn.edu.thu.tsfiledb.qp.exception.IllegalASTFormatException;
import cn.edu.thu.tsfiledb.qp.exception.QueryProcessorException;
import cn.edu.thu.tsfiledb.qp.executor.OverflowQPExecutor;
import cn.edu.thu.tsfiledb.qp.physical.PhysicalPlan;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSExecuteStatementResp;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSHandleIdentifier;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSOperationHandle;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TS_StatusCode;
import cn.edu.thu.tsfiledb.sys.writelog.WriteLogManager;

public class Test {

	public static void main(String[] args) {
		QueryProcessor processor = new QueryProcessor(new OverflowQPExecutor());
		PhysicalPlan plan = null;
		try {
			plan = processor.parseSQLToPhysicalPlan("delete timeseries root.vehicle,root.vehicle.d0.s1");
		} catch (QueryProcessorException | ArgsErrorException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		List<Path> paths = plan.getPaths();
		try {
			processor.getExecutor().processNonQuery(plan);
		} catch (ProcessorException e) {
			
		}

	}

}
