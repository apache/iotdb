package cn.edu.thu.tsfiledb.sys.writeLog;

import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfile.common.conf.TSFileConfig;
import cn.edu.thu.tsfile.common.conf.TSFileDescriptor;
import cn.edu.thu.tsfiledb.conf.TSFileDBConfig;
import cn.edu.thu.tsfiledb.conf.TSFileDBDescriptor;
import cn.edu.thu.tsfiledb.qp.logical.operator.Operator.OperatorType;
import cn.edu.thu.tsfiledb.qp.physical.plan.PhysicalPlan;
import cn.edu.thu.tsfiledb.sys.writeLog.impl.LocalFileLogReader;
import cn.edu.thu.tsfiledb.sys.writeLog.impl.LocalFileLogWriter;

public class WriteLogManager {
	private static final Logger logger = LoggerFactory.getLogger(WriteLogManager.class);
	private static WriteLogManager instance;
	private PhysicalPlanLogTransfer transfer = new PhysicalPlanLogTransfer();
	private WriteLogReadable reader; 
	private WriteLogPersistable writer = null;
	private TSFileDBConfig config = TSFileDBDescriptor.getInstance().getConfig();	
	private String logFile;
	
	private WriteLogManager() {
		logFile = config.writeLogPath;
	}
	
	public void write(PhysicalPlan plan) throws IOException {
//		if (writer == null) {
//			writer = new LocalFileLogWriter(logFile);
//		}
//		writer.write(transfer.operatorToLog(plan));
	}

	public void overflowFlush() throws IOException {
		if (writer == null) {
			writer = new LocalFileLogWriter(logFile);
		}

		byte[] flushStart = new byte[1];
		flushStart[0] = (byte) OperatorType.OVERFLOWFLUSHSTART.ordinal();
		writer.write(flushStart);

		byte[] flushEnd = new byte[1];
		flushEnd[0] = (byte) OperatorType.OVERFLOWFLUSHEND.ordinal();
		writer.write(flushEnd);
//		writer.close();
//		writer = null;
	}

	public void bufferFlush() throws IOException {
		if (writer == null) {
			writer = new LocalFileLogWriter(logFile);
		}

		byte[] flushStart = new byte[1];
		flushStart[0] = (byte) OperatorType.BUFFERFLUSHSTART.ordinal();
		writer.write(flushStart);

		byte[] flushEnd = new byte[1];
		flushEnd[0] = (byte) OperatorType.BUFFERFLUSHEND.ordinal();
		writer.write(flushEnd);
//		writer.close();
//		writer = null;
		logger.info("write flush log for bufferedWrite done.");
	}
	

	/**
	 * may cause errors in multi processors?
	 *
	 * @return
	 */
	public PhysicalPlan getPhysicalPlan() throws IOException {
		if (reader == null) {
			reader = new LocalFileLogReader(logFile);
		}
		PhysicalPlan ans = reader.getPhysicalPlan();
		return ans;
	}
	
	public static WriteLogManager getInstance(){
		if(instance == null){
			synchronized (WriteLogManager.class) {
				instance = new WriteLogManager();
			}
		}
		return instance;
	}

	public void reset() throws IOException {
		File f = new File(logFile);
		if (f.exists())
			f.delete();
	}
}
