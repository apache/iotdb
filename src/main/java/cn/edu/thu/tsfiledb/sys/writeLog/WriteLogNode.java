package cn.edu.thu.tsfiledb.sys.writeLog;

import cn.edu.thu.tsfile.common.utils.BytesUtils;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfiledb.qp.logical.operator.Operator;
import cn.edu.thu.tsfiledb.qp.physical.plan.PhysicalPlan;
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
 *
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
    private String filePath;
    private String backFilePath;
    private int logSize;
    private Path path;
    private List<PhysicalPlan> plansInMemory;

    public WriteLogNode(Path path) {
        this.path = path;
        filePath = "src/main/resources/log/" + path + ".log";
        plansInMemory = new ArrayList<>();
        hasBufferWriteFlush = false;
        hasOverflowFlush = false;
        logSize = 0;
    }

    synchronized public void write(PhysicalPlan plan) throws IOException {
        plansInMemory.add(plan);
        if (plansInMemory.size() >= SystemLogConfig.LogMemorySize) {
            logSize += plansInMemory.size();
            serializeMemoryToFile();
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
        writer.write(BytesUtils.intToTwoBytes(flushStart.length)); // 2 bytes to represent the content size
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
        writer.write(BytesUtils.intToTwoBytes(flushEnd.length));
        hasOverflowFlush = true;
        LOG.info("Write overflow log end.");
    }

    synchronized public void bufferFlushStart() throws IOException {
        serializeMemoryToFile();

        if (writer == null) {
            writer = new LocalFileLogWriter(filePath);
        }
        byte[] flushStart = new byte[1];
        flushStart[0] = (byte) Operator.OperatorType.BUFFERFLUSHSTART.ordinal();
        writer.write(flushStart);
        writer.write(BytesUtils.intToTwoBytes(flushStart.length));
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
        writer.write(BytesUtils.intToTwoBytes(flushEnd.length));
        LOG.info("Write bufferwrite log end.");
        hasBufferWriteFlush = true;
//		writer.close();
//		writer = null;
    }

    synchronized public void checkFileSize() throws IOException {
        if (logSize >= SystemLogConfig.LogMergeSize && hasBufferWriteFlush || hasOverflowFlush) {
            LocalFileLogWriter writerV2 = new LocalFileLogWriter(filePath + ".backup");
            LocalFileLogReader oldReader = new LocalFileLogReader(filePath);
            writerV2.write(oldReader.getFileCompactData());
            new File(filePath).delete();
            new File(filePath + ".backup").renameTo(new File(filePath));
            logSize = 0;
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
        byte[] bytesToSerialize = new byte[totalBytes + 2*plansInMemory.size()];
        int pos = 0;
        for (byte[] bs : bytesInMemory) {
            System.arraycopy(bs, 0, bytesToSerialize, pos, bs.length);
            pos += bs.length;
            byte[] len = BytesUtils.intToTwoBytes(bs.length);
            System.arraycopy(len, 0, bytesToSerialize, pos, len.length);
            pos += 2;
        }

        if (writer == null) {
            writer = new LocalFileLogWriter(filePath);
        }
        writer.write(bytesToSerialize);
        plansInMemory.clear();
    }

    synchronized public void recovery() {

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

        return reader.getPhysicalPlan();
    }


    public void resetFileStatus() throws IOException {
        File f = new File(filePath);
        if (f.exists())
            f.delete();
    }
}
