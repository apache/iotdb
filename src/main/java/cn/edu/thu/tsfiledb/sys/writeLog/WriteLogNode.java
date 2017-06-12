package cn.edu.thu.tsfiledb.sys.writeLog;

import cn.edu.thu.tsfile.common.utils.BytesUtils;
import cn.edu.thu.tsfile.timeseries.write.record.DataPoint;
import cn.edu.thu.tsfile.timeseries.write.record.TSRecord;
import cn.edu.thu.tsfiledb.conf.TsfileDBDescriptor;
import cn.edu.thu.tsfiledb.qp.logical.Operator;
import cn.edu.thu.tsfiledb.qp.physical.crud.MultiInsertPlan;
import cn.edu.thu.tsfiledb.qp.physical.PhysicalPlan;
import cn.edu.thu.tsfiledb.sys.writeLog.impl.LocalFileLogReader;
import cn.edu.thu.tsfiledb.sys.writeLog.impl.LocalFileLogWriter;
import cn.edu.thu.tsfiledb.sys.writeLog.transfer.PhysicalPlanLogTransfer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * System log persist interface.
 *
 * @author CGF
 */
public class WriteLogNode {

    private static final Logger LOG = LoggerFactory.getLogger(WriteLogNode.class);
    private PhysicalPlanLogTransfer transfer = new PhysicalPlanLogTransfer();
    private WriteLogReadable reader;
    private WriteLogPersistable writer = null;
    // private TSFileDBConfig config = TSFileDBDescriptor.getInstance().getConfig();
    private boolean hasBufferWriteFlush = false, hasOverflowFlush = false;
    private String filePath, backFilePath;
    private int LogCompactSize, LogMemorySize;
    private int logSize;
    private String path;
    private List<PhysicalPlan> plansInMemory;

    public WriteLogNode(String path) {
        this.path = path;
        this.LogCompactSize = TsfileDBDescriptor.getInstance().getConfig().LogCompactSize;
        this.LogMemorySize = TsfileDBDescriptor.getInstance().getConfig().LogMemorySize;
        filePath = TsfileDBDescriptor.getInstance().getConfig().walFolder + path + ".log";
        backFilePath = filePath + ".backup";
        plansInMemory = new ArrayList<>();
        hasBufferWriteFlush = false;
        hasOverflowFlush = false;
        logSize = 0;
    }

    public void setLogCompactSize(int size) {
        this.LogCompactSize = size;
    }

    public void setLogMemorySize(int size) {
        this.LogMemorySize = size;
    }

    synchronized public void write(PhysicalPlan plan) throws IOException {
        plansInMemory.add(plan);
        if (plansInMemory.size() >= LogMemorySize) {
            serializeMemoryToFile();
            logSize += plansInMemory.size();
            checkLogsCompactFileSize(false);
        }
    }

    synchronized public void write(TSRecord record, int flag) throws IOException {
        if (flag == WriteLogManager.OVERFLOW) {
            List<String> measurementList = new ArrayList<>();
            List<String> insertValues = new ArrayList<>();
            for (DataPoint dp : record.dataPointList) {
                measurementList.add(dp.getMeasurementId());
                insertValues.add(dp.getValue().toString());
            }
            plansInMemory.add(new MultiInsertPlan(2, record.deltaObjectId, record.time, measurementList, insertValues));
        } else if (flag == WriteLogManager.BUFFERWRITER) {
            List<String> measurementList = new ArrayList<>();
            List<String> insertValues = new ArrayList<>();
            for (DataPoint dp : record.dataPointList) {
                measurementList.add(dp.getMeasurementId());
                insertValues.add(dp.getValue().toString());
            }
            plansInMemory.add(new MultiInsertPlan(1, record.deltaObjectId, record.time, measurementList, insertValues));
        }
        if (plansInMemory.size() >= LogMemorySize) {
            serializeMemoryToFile();
            logSize += plansInMemory.size();
            checkLogsCompactFileSize(false);
        }
    }

    synchronized public void overflowFlushStart() throws IOException {
        serializeMemoryToFile();

        if (writer == null) {
            writer = new LocalFileLogWriter(filePath);
        }
        byte[] flushStart = new byte[1];
        flushStart[0] = (byte) Operator.OperatorType.OVERFLOWFLUSHSTART.ordinal();
        writer.write(flushStart);
        writer.write(BytesUtils.intToBytes(flushStart.length)); // 2 bytes to represent the content size
        LOG.info("Write overflow log start.");
    }

    synchronized public void overflowFlushEnd() throws IOException {
        serializeMemoryToFile();

        if (writer == null) {
            writer = new LocalFileLogWriter(filePath);
        }
        byte[] flushEnd = new byte[1];
        flushEnd[0] = (byte) Operator.OperatorType.OVERFLOWFLUSHEND.ordinal();
        writer.write(flushEnd);
        writer.write(BytesUtils.intToBytes(flushEnd.length));
        hasOverflowFlush = true;
        LOG.info("Write overflow log end.");
        checkLogsCompactFileSize(false);
    }

    synchronized public void bufferFlushStart() throws IOException {
        serializeMemoryToFile();

        if (writer == null) {
            writer = new LocalFileLogWriter(filePath);
        }
        byte[] flushStart = new byte[1];
        flushStart[0] = (byte) Operator.OperatorType.BUFFERFLUSHSTART.ordinal();
        writer.write(flushStart);
        writer.write(BytesUtils.intToBytes(flushStart.length));
        LOG.info("Write bufferwrite log start.");
    }

    synchronized public void bufferFlushEnd() throws IOException {
        serializeMemoryToFile();

        if (writer == null) {
            writer = new LocalFileLogWriter(filePath);
        }
        byte[] flushEnd = new byte[1];
        flushEnd[0] = (byte) Operator.OperatorType.BUFFERFLUSHEND.ordinal();
        writer.write(flushEnd);
        writer.write(BytesUtils.intToBytes(flushEnd.length));
        LOG.info("Write bufferwrite log end.");
        hasBufferWriteFlush = true;
        checkLogsCompactFileSize(false);
//		writer.close();
//		writer = null;
    }

    /**
     * Compact logs in path.log.
     *
     * @throws IOException
     */
    synchronized public void checkLogsCompactFileSize(boolean forceCompact) throws IOException {
        if (logSize >= LogCompactSize && hasBufferWriteFlush ||
                (logSize >= LogCompactSize && hasOverflowFlush) || forceCompact) {
            LOG.info("Log Compact Process Begin.");
            LocalFileLogWriter writerV2 = new LocalFileLogWriter(backFilePath);
            LocalFileLogReader oldReader = new LocalFileLogReader(filePath);
            writerV2.write(oldReader.getFileCompactData());
            new File(filePath).delete();
            new File(filePath + ".backup").renameTo(new File(filePath));
            //writer = new LocalFileLogWriter(filePath);
            writer = null;
            logSize = 0;
            hasBufferWriteFlush = false;
            hasOverflowFlush = false;
            LOG.info("Log Compact Process End.");
        }
    }

    synchronized private void serializeMemoryToFile() throws IOException {
        if (plansInMemory.size() == 0)
            return;

        int totalBytes = 0;
        List<byte[]> bytesInMemory = new ArrayList<>();
        for (PhysicalPlan plan : plansInMemory) {
            byte[] planBytes = PhysicalPlanLogTransfer.operatorToLog(plan);
            bytesInMemory.add(planBytes);
            totalBytes += planBytes.length;
        }
        byte[] bytesToSerialize = new byte[totalBytes + 4 * plansInMemory.size()];
        int pos = 0;
        for (byte[] bs : bytesInMemory) {
            System.arraycopy(bs, 0, bytesToSerialize, pos, bs.length);
            pos += bs.length;
            byte[] len = BytesUtils.intToBytes(bs.length);
            System.arraycopy(len, 0, bytesToSerialize, pos, len.length);
            pos += 4;
        }

        if (writer == null) {
            writer = new LocalFileLogWriter(filePath);
        }
        writer.write(bytesToSerialize);
        logSize += plansInMemory.size();
        plansInMemory.clear();
    }

    synchronized public void recovery() throws IOException {
        File f = new File(backFilePath);
        if (f.exists()) {
            LOG.error("compact error!!!");
            // need delete origin file
            f.delete();
            checkLogsCompactFileSize(true);
        }
    }

    /**
     * may cause errors in multi processors
     *
     * @return
     */
    synchronized public PhysicalPlan getPhysicalPlan() throws IOException {
        if (reader == null) {
            reader = new LocalFileLogReader(filePath);
        }

        PhysicalPlan plan = reader.getPhysicalPlan();
        if (plan != null) {
            logSize++;
        }
        return plan;
    }


    public void resetFileStatus() throws IOException {
        File f = new File(filePath);
        if (f.exists())
            f.delete();
    }
}
