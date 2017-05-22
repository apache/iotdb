package cn.edu.thu.tsfiledb.sys.writeLog.impl;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import cn.edu.thu.tsfile.common.utils.BytesUtils;
import cn.edu.thu.tsfiledb.qp.logical.operator.Operator.OperatorType;
import cn.edu.thu.tsfiledb.qp.physical.plan.PhysicalPlan;
import cn.edu.thu.tsfiledb.sys.writeLog.transfer.PhysicalPlanLogTransfer;
import cn.edu.thu.tsfiledb.sys.writeLog.WriteLogReadable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author CGF
 */
public class LocalFileLogReader implements WriteLogReadable {
    private static final Logger LOG = LoggerFactory.getLogger(LocalFileLogReader.class);
    private static String fileName = "";
    private RandomAccessFile raf = null;
    private long pos = 0;
    private long fileLength = 0;
    private boolean fileExist;

    public LocalFileLogReader(String file) throws IOException {
        fileExist = true;
        fileName = file;
        try {
            raf = new RandomAccessFile(file, "rw");
        } catch (FileNotFoundException e) {
            fileExist = false;
        }
        fileLength = raf.length();
        pos = fileLength;
    }

    @Override
    public boolean hasNextOperator() throws IOException {
        if (!fileExist) {
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
        } else if (opeType == 25) { // FLUSHSTART
            return false;
        } else if (opeType == 26) { // FLUSHEND
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
        // -1 : no end, no start
        // 1 : has end
        // 2 : has start and end
        // 3 : only has start
        int overflowVis = -1;
        int bufferVis = -1;

        while (i > 0) {
            lraf.seek(i - 2);
            byte[] opeContentLengthBytes = new byte[2];
            lraf.read(opeContentLengthBytes);
            int opeContentLength = BytesUtils.twoBytesToInt(opeContentLengthBytes);

            byte[] opeTypeBytes = new byte[1];
            lraf.seek(i - 2 - opeContentLength);
            lraf.read(opeTypeBytes);
            int opeType = (int) opeTypeBytes[0];

            if (opeType == OperatorType.OVERFLOWFLUSHEND.ordinal()) {
                overflowVis = 1;
                i -= (2 + opeContentLength);
                continue;
            } else if (opeType == OperatorType.OVERFLOWFLUSHSTART.ordinal()) {
                if (overflowVis == 1)
                    overflowVis = 2;
                else
                    overflowVis = 3;
                i -= (2 + opeContentLength);
                continue;
            } else if (opeType == OperatorType.BUFFERFLUSHEND.ordinal()) {
                bufferVis = 1;
                i -= (2 + opeContentLength);
                continue;
            } else if (opeType == OperatorType.BUFFERFLUSHSTART.ordinal()) {
                if (bufferVis == 1)
                    bufferVis = 2;
                else
                    bufferVis = 3;
                i -= (2 + opeContentLength);
                continue;
            }

            if (bufferVis == 2 && overflowVis == 2) {
                break;
            }

            if (opeType == OperatorType.MULTIINSERT.ordinal() || opeType == OperatorType.INSERT.ordinal()) {
                byte[] insertTypeBytes = new byte[1];
                lraf.read(insertTypeBytes);
                int insertType = (int) insertTypeBytes[0];
                if (insertType == 1 && bufferVis != 2) {  // bufferwrite insert
                    bufferStartList.add(i - 2 - opeContentLength);
                    bufferLengthList.add(opeContentLength);
                    bufferTailCount++;
                } else if (insertType == 2 && overflowVis != 2) {     // overflow insert
                    overflowStartList.add(i - 2 - opeContentLength);
                    overflowLengthList.add(opeContentLength);
                    overflowTailCount++;
                }
            } else if (overflowVis != 2) { // overflow update/delete
                overflowStartList.add(i - 2 - opeContentLength);
                overflowLengthList.add(opeContentLength);
                overflowTailCount++;
            }
            i -= (2 + opeContentLength);
        }
    }

    @Override
    public PhysicalPlan getPhysicalPlan() throws IOException {
        if (tailPos == -1) {
            getStartPos();
        }

        if (bufferTailCount == 0 && overflowTailCount == 0) {
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

        if (overflowStart == -1 || (bufferStart < overflowStart) && bufferTailCount > 0) { // overflow operator is empty OR buffer operator is in front of overflow
            lraf.seek(bufferStart);
            byte[] planBytes = new byte[bufferLength];
            lraf.read(planBytes);
            bufferTailCount--;
            return PhysicalPlanLogTransfer.logToOperator(planBytes);
        } else {
            lraf.seek(overflowStart);
            byte[] planBytes = new byte[overflowLength];
            lraf.read(planBytes);
            overflowTailCount--;
            return PhysicalPlanLogTransfer.logToOperator(planBytes);
        }
    }

    public byte[] getFileCompactData() throws IOException {
        tailPos = 0;
        lraf = new RandomAccessFile(fileName, "rw");
        int i = (int) lraf.length();
        // -1 : no end, no start
        // 1 : has end
        // 2 : has start and end
        // 3 : only has start
        int overflowVis = -1;
        int bufferVis = -1;
        List<byte[]> backUpBytesList = new ArrayList<>();
        int backUpTotalLength = 0;

        while (i > 0) {
            lraf.seek(i - 2);
            byte[] opeContentLengthBytes = new byte[2];
            lraf.read(opeContentLengthBytes);
            int opeContentLength = BytesUtils.twoBytesToInt(opeContentLengthBytes);

            byte[] opeTypeBytes = new byte[1];
            int backUpPos = i - 2 - opeContentLength;
            lraf.seek(backUpPos);
            lraf.read(opeTypeBytes);
            int opeType = (int) opeTypeBytes[0];

            if (opeType == OperatorType.OVERFLOWFLUSHEND.ordinal()) {
                overflowVis = 1;
                i -= (2 + opeContentLength);
                continue;
            } else if (opeType == OperatorType.OVERFLOWFLUSHSTART.ordinal()) {
                if (overflowVis == 1)
                    overflowVis = 2;
                else
                    overflowVis = 3;
                i -= (2 + opeContentLength);
                continue;
            } else if (opeType == OperatorType.BUFFERFLUSHEND.ordinal()) {
                bufferVis = 1;
                i -= (2 + opeContentLength);
                continue;
            } else if (opeType == OperatorType.BUFFERFLUSHSTART.ordinal()) {
                if (bufferVis == 1)
                    bufferVis = 2;
                else
                    bufferVis = 3;
                i -= (2 + opeContentLength);
                continue;
            }

            if (bufferVis == 2 && overflowVis == 2) {
                break;
            }

            byte[] dataBackUp = new byte[opeContentLength + 2];
            lraf.seek(backUpPos);
            lraf.read(dataBackUp);
            backUpBytesList.add(dataBackUp);
            backUpTotalLength += dataBackUp.length;
            i -= (2 + opeContentLength);
        }

        byte[] ans = new byte[backUpTotalLength];
        int pos = 0;
        for (i = backUpBytesList.size() - 1; i >= 0; i--) {
            byte[] dataBackUp = backUpBytesList.get(i);
            System.arraycopy(dataBackUp, 0, ans, pos, dataBackUp.length);
            pos += dataBackUp.length;
        }

        return ans;
//        List<byte[]> bytesList = new ArrayList<>();
//        int totalLength = 0;
//git
//        getStartPos();
//
//        if (bufferTailCount == 0 && overflowTailCount == 0) {
//            tailPos = -1;
//            return null;
//        }
//
//        int overflowStart = -1, overflowLength = -1;
//        int bufferStart = -1, bufferLength = -1;
//
//        if (bufferTailCount > 0) {
//            bufferStart = bufferStartList.get(bufferTailCount - 1);
//            bufferLength = bufferLengthList.get(bufferTailCount - 1);
//        }
//        if (overflowTailCount > 0) {
//            overflowStart = overflowStartList.get(overflowTailCount - 1);
//            overflowLength = overflowLengthList.get(overflowTailCount - 1);
//        }
//
//        // LOG.info(fileLength + ", " + overflowStart + ":" + overflowLength + ", " + bufferStart + ":" + bufferLength);
//
//        if (overflowStart == -1 || (bufferStart < overflowStart) && bufferTailCount > 0) { // overflow operator is empty OR buffer operator is in front of overflow
//            lraf.seek(bufferStart);
//            byte[] planBytes = new byte[bufferLength];
//            lraf.read(planBytes);
//            bufferTailCount--;
//            bytesList.add(planBytes);
//            totalLength += planBytes.length;
//        } else {
//            lraf.seek(overflowStart);
//            byte[] planBytes = new byte[overflowLength];
//            lraf.read(planBytes);
//            overflowTailCount--;
//            bytesList.add(planBytes);
//            totalLength += planBytes.length;
//        }
//
//        byte[] ans = new byte[totalLength];
//        int pos = 0;
//        for (byte[] bs : bytesList) {
//            System.arraycopy(bs, 0, ans, pos, bs.length);
//            pos += bs.length;
//        }
//        return ans;
    }
}
