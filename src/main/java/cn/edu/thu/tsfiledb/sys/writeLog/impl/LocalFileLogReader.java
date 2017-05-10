package cn.edu.thu.tsfiledb.sys.writeLog.impl;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import cn.edu.thu.tsfile.common.utils.BytesUtils;
import cn.edu.thu.tsfiledb.qp.logical.operator.Operator;
import cn.edu.thu.tsfiledb.qp.logical.operator.Operator.OperatorType;
import cn.edu.thu.tsfiledb.qp.physical.plan.PhysicalPlan;
import cn.edu.thu.tsfiledb.sys.writeLog.PhysicalPlanLogTransfer;
import cn.edu.thu.tsfiledb.sys.writeLog.WriteLogReadable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author CGF
 */
public class LocalFileLogReader implements WriteLogReadable{
	private static final Logger LOG = LoggerFactory.getLogger(LocalFileLogReader.class);
	private static String fileName = "";
	private RandomAccessFile raf = null;
	private long pos = 0;
	private long fileLength = 0;
	private boolean fileExist;
	
	public LocalFileLogReader(String file) throws IOException {
		fileExist = true;
		fileName = file;
		try{
			raf = new RandomAccessFile(file, "rw");
		}catch(FileNotFoundException e){
			fileExist = false;
		}
		fileLength = raf.length();
		pos = fileLength;
	}
	
	@Override
	public boolean hasNextOperator() throws IOException {
		if(!fileExist){
			return false;
		}
		if (pos <= 0) {
			return false;
		}
		raf.seek(pos - 2);
		byte[] opeContentLengthBytes = new byte[2];
		raf.read(opeContentLengthBytes);
		int opeContentLength = BytesUtils.twoBytesToInt(opeContentLengthBytes);

		byte[] opeTypeBytes = new byte[1];
		raf.seek(pos - 2 - opeContentLength);
		raf.read(opeTypeBytes);
		int opeType = (int) opeTypeBytes[0];

		if (opeType == OperatorType.INSERT.ordinal() || opeType == OperatorType.UPDATE.ordinal() ||
				opeType == OperatorType.MULTIINSERT.ordinal() || opeType == OperatorType.DELETE.ordinal()) { // INSERT UPDATE DELETE OPERATOR
			return true;
		} else if(opeType == 25){ // FLUSHSTART
			return false;
		} else if(opeType == 26) { // FLUSHEND
			return false;
		}
		return false;
	}

	@Override
	public byte[] nextOperator() throws IOException {

		raf.seek(pos - 2);
		byte[] opeContentLengthBytes = new byte[2];
		raf.read(opeContentLengthBytes);
		int opeContentLength = BytesUtils.twoBytesToInt(opeContentLengthBytes);

		byte[] opeContent = new byte[opeContentLength];
		raf.seek(pos - 2 - opeContentLength);
		raf.read(opeContent);

		pos = pos - 2 - opeContentLength;
		return opeContent;
	}

	private int tailPos = -1;
	private int overflowTailCount = 0;
	private int bufferTailCount = 0;
	private static List<Integer> overflowLengthList = new ArrayList<>();
	private static List<Integer> bufferLengthList = new ArrayList<>();
	private static List<Integer> overflowStartList = new ArrayList<>();
	private static List<Integer> bufferStartList = new ArrayList<>();
	private RandomAccessFile lraf = null;

	private void getStartPos() throws IOException {
		tailPos = 0;
		lraf = new RandomAccessFile(fileName, "rw");
		int i = (int) lraf.length();
		boolean overflowVis = true;
		boolean bufferVis = true;

		while (i > 0) {
			lraf.seek(i-2);
			byte[] opeContentLengthBytes = new byte[2];
			lraf.read(opeContentLengthBytes);
			int opeContentLength = BytesUtils.twoBytesToInt(opeContentLengthBytes);

			byte[] opeTypeBytes = new byte[1];
			lraf.seek(i - 2 - opeContentLength);
			lraf.read(opeTypeBytes);
			int opeType = (int) opeTypeBytes[0];

			if (opeType == OperatorType.OVERFLOWFLUSHSTART.ordinal() || opeType == OperatorType.OVERFLOWFLUSHEND.ordinal()) {
				overflowVis = false;
				// need continue
				i -= (2 + opeContentLength);
				continue;
			}
			if (opeType == OperatorType.BUFFERFLUSHSTART.ordinal() || opeType == OperatorType.BUFFERFLUSHEND.ordinal()) {
				bufferVis = false;
				// need continue
				i -= (2 + opeContentLength);
				continue;
			}
			if (!bufferVis && !overflowVis) {
				break;
			}

			if (opeType == OperatorType.INSERT.ordinal() || opeType == OperatorType.MULTIINSERT.ordinal() && bufferVis) {
				byte[] insertTypeBytes = new byte[1];
				lraf.read(insertTypeBytes);
				int insertType = (int) insertTypeBytes[0];
				if (insertType == 1) {
					bufferStartList.add(i - 2 - opeContentLength);
					bufferLengthList.add(opeContentLength);
					bufferTailCount ++;
				} else if (overflowVis){
					overflowStartList.add(i - 2 - opeContentLength);
					overflowLengthList.add(opeContentLength);
					overflowTailCount ++;
				}

			} else if (overflowVis){
				overflowStartList.add(i - 2 - opeContentLength);
				overflowLengthList.add(opeContentLength);
				overflowTailCount ++;
			}
			i -= (2 + opeContentLength);
		}
	}

	@Override
	public PhysicalPlan getPhysicalPlan() throws IOException {
		if (tailPos == -1) {
			getStartPos();
		}

		if	(bufferTailCount == 0 && overflowTailCount == 0) {
			tailPos = -1;
			return null;
		}

		int overflowStart = -1, overflowLength = -1;
		int bufferStart = -1, bufferLength = -1;

		if (bufferTailCount > 0) {
			bufferStart = bufferStartList.get(bufferTailCount - 1);
			bufferLength = bufferLengthList.get(bufferTailCount - 1);
		}
		if (overflowTailCount > 0) {
			overflowStart = overflowStartList.get(overflowTailCount - 1);
			overflowLength = overflowLengthList.get(overflowTailCount - 1);
		}


		LOG.info(fileLength + ", " + overflowStart + ":" + overflowLength + ", " + bufferStart + ":" + bufferLength);

		if (overflowStart == -1 || (bufferStart < overflowStart) && bufferTailCount > 0) { // overflow operator is empty OR buffer operator is in front of ovf
			lraf.seek(bufferStart);
			byte[] planBytes = new byte[bufferLength];
			lraf.read(planBytes);
			bufferTailCount --;
			return PhysicalPlanLogTransfer.logToOperator(planBytes);


		} else {

			lraf.seek(overflowStart);
			byte[] planBytes = new byte[overflowLength];
			lraf.read(planBytes);
			overflowTailCount --;
			return PhysicalPlanLogTransfer.logToOperator(planBytes);

		}
	}

}
