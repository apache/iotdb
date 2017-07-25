package cn.edu.thu.tsfiledb.sys.writelog.impl;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import cn.edu.thu.tsfile.common.utils.BytesUtils;
import cn.edu.thu.tsfiledb.qp.logical.Operator.OperatorType;
import cn.edu.thu.tsfiledb.qp.physical.PhysicalPlan;
import cn.edu.thu.tsfiledb.sys.writelog.transfer.PhysicalPlanLogTransfer;
import cn.edu.thu.tsfiledb.sys.writelog.WriteLogReadable;

import cn.edu.thu.tsfiledb.sys.writelog.transfer.SystemLogOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author CGF
 */
public class LocalFileLogReader implements WriteLogReadable {
    private static final Logger LOG = LoggerFactory.getLogger(LocalFileLogReader.class);
    private String fileName = "";
    private int tailPos = -1;
    private int overflowTailCount = 0;
    private int bufferTailCount = 0;
    private List<Integer> overflowLengthList = new ArrayList<>();
    private List<Integer> bufferLengthList = new ArrayList<>();
    private List<Integer> overflowStartList = new ArrayList<>();
    private List<Integer> bufferStartList = new ArrayList<>();
    private RandomAccessFile lraf = null;

    public LocalFileLogReader(String fileName) throws IOException {
        this.fileName = fileName;
        File f = new File(fileName);
        if (!f.getParentFile().exists()) {
            f.getParentFile().mkdirs();
        }
    }

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
            lraf.seek(i - 4);
            byte[] opeContentLengthBytes = new byte[4];
            lraf.read(opeContentLengthBytes);
            int opeContentLength = BytesUtils.bytesToInt(opeContentLengthBytes);

            byte[] opeTypeBytes = new byte[1];
            lraf.seek(i - 4 - opeContentLength);
            lraf.read(opeTypeBytes);
            int opeType = (int) opeTypeBytes[0];

            if (opeType == SystemLogOperator.OVERFLOWFLUSHEND) {
                overflowVis = 1;
                i -= (4 + opeContentLength);
                continue;
            } else if (opeType == SystemLogOperator.OVERFLOWFLUSHSTART) {
                if (overflowVis == 1)
                    overflowVis = 2;
                else
                    overflowVis = 3;
                i -= (4 + opeContentLength);
                continue;
            } else if (opeType == SystemLogOperator.BUFFERFLUSHEND) {
                bufferVis = 1;
                i -= (4 + opeContentLength);
                continue;
            } else if (opeType == SystemLogOperator.BUFFERFLUSHSTART) {
                if (bufferVis == 1)
                    bufferVis = 2;
                else
                    bufferVis = 3;
                i -= (4 + opeContentLength);
                continue;
            }

            if (bufferVis == 2 && overflowVis == 2) {
                break;
            }

            if (opeType == SystemLogOperator.INSERT) {
                byte[] insertTypeBytes = new byte[1];
                lraf.read(insertTypeBytes);
                int insertType = (int) insertTypeBytes[0];
                if (insertType == 1 && bufferVis != 2) {  // bufferwrite insert
                    bufferStartList.add(i - 4 - opeContentLength);
                    bufferLengthList.add(opeContentLength);
                    bufferTailCount++;
                } else if (insertType == 2 && overflowVis != 2) {     // overflow insert
                    overflowStartList.add(i - 4 - opeContentLength);
                    overflowLengthList.add(opeContentLength);
                    overflowTailCount++;
                }
            } else if (overflowVis != 2) { // overflow update/delete
                overflowStartList.add(i - 4 - opeContentLength);
                overflowLengthList.add(opeContentLength);
                overflowTailCount++;
            }
            i -= (4 + opeContentLength);
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

        // LOG.debug(fileLength + ", " + overflowStart + ":" + overflowLength + ", " + bufferStart + ":" + bufferLength);

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
            lraf.seek(i - 4);
            byte[] opeContentLengthBytes = new byte[4];
            lraf.read(opeContentLengthBytes);
            int opeContentLength = BytesUtils.bytesToInt(opeContentLengthBytes);

            byte[] opeTypeBytes = new byte[1];
            int backUpPos = i - 4 - opeContentLength;
            lraf.seek(backUpPos);
            lraf.read(opeTypeBytes);
            int opeType = (int) opeTypeBytes[0];

            if (opeType == SystemLogOperator.OVERFLOWFLUSHEND) {
                overflowVis = 1;
                i -= (4 + opeContentLength);
                continue;
            } else if (opeType == SystemLogOperator.OVERFLOWFLUSHSTART) {
                if (overflowVis == 1)
                    overflowVis = 2;
                else
                    overflowVis = 3;
                i -= (4 + opeContentLength);
                continue;
            } else if (opeType == SystemLogOperator.BUFFERFLUSHEND) {
                bufferVis = 1;
                i -= (4 + opeContentLength);
                continue;
            } else if (opeType == SystemLogOperator.BUFFERFLUSHSTART) {
                if (bufferVis == 1)
                    bufferVis = 2;
                else
                    bufferVis = 3;
                i -= (4 + opeContentLength);
                continue;
            }

            if (bufferVis == 2 && overflowVis == 2) {
                break;
            }

            if (opeType == SystemLogOperator.INSERT) {
                byte[] insertTypeBytes = new byte[1];
                lraf.read(insertTypeBytes);
                int insertType = (int) insertTypeBytes[0];
                if (insertType == 1 && bufferVis != 2) {  // bufferwrite insert
                    byte[] dataBackUp = new byte[opeContentLength + 4];
                    lraf.seek(backUpPos);
                    lraf.read(dataBackUp);
                    backUpBytesList.add(dataBackUp);
                    backUpTotalLength += dataBackUp.length;
                } else if (insertType == 2 && overflowVis != 2) {     // overflow insert
                    byte[] dataBackUp = new byte[opeContentLength + 4];
                    lraf.seek(backUpPos);
                    lraf.read(dataBackUp);
                    backUpBytesList.add(dataBackUp);
                    backUpTotalLength += dataBackUp.length;
                }
            } else if (overflowVis != 2) { // overflow update/delete
                byte[] dataBackUp = new byte[opeContentLength + 4];
                lraf.seek(backUpPos);
                lraf.read(dataBackUp);
                backUpBytesList.add(dataBackUp);
                backUpTotalLength += dataBackUp.length;
            }
            i -= (4 + opeContentLength);
        }

        byte[] ans = new byte[backUpTotalLength];
        int pos = 0;
        for (i = backUpBytesList.size() - 1; i >= 0; i--) {
            byte[] dataBackUp = backUpBytesList.get(i);
            System.arraycopy(dataBackUp, 0, ans, pos, dataBackUp.length);
            pos += dataBackUp.length;
        }

        return ans;
    }

    @Override
    public void close() throws IOException {
        if (lraf != null) {
            lraf.close();
            lraf = null;
        }
    }
}
