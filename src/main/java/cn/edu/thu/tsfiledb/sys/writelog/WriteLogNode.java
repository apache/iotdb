package cn.edu.thu.tsfiledb.sys.writelog;

import cn.edu.thu.tsfile.common.utils.BytesUtils;
import cn.edu.thu.tsfile.timeseries.write.record.DataPoint;
import cn.edu.thu.tsfile.timeseries.write.record.TSRecord;
import cn.edu.thu.tsfiledb.conf.TsfileDBDescriptor;
import cn.edu.thu.tsfiledb.qp.logical.Operator;
import cn.edu.thu.tsfiledb.qp.physical.PhysicalPlan;
import cn.edu.thu.tsfiledb.qp.physical.crud.InsertPlan;
import cn.edu.thu.tsfiledb.sys.writelog.impl.LocalFileLogReader;
import cn.edu.thu.tsfiledb.sys.writelog.impl.LocalFileLogWriter;
import cn.edu.thu.tsfiledb.sys.writelog.transfer.PhysicalPlanLogTransfer;
import cn.edu.thu.tsfiledb.sys.writelog.transfer.SystemLogOperator;
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
    private WriteLogReadable reader;
    private WriteLogPersistable writer = null;
    // private TSFileDBConfig config = TSFileDBDescriptor.getInstance().getConfig();
    private boolean hasBufferWriteFlush = false, hasOverflowFlush = false;
    private String filePath, backFilePath;
    private int LogCompactSize, LogMemorySize;
    private int logSize;
    private String path;
    private List<PhysicalPlan> plansInMemory;

    WriteLogNode(String path) {
        this.path = path;
        this.LogCompactSize = TsfileDBDescriptor.getInstance().getConfig().walCleanupThreshold;
        this.LogMemorySize = TsfileDBDescriptor.getInstance().getConfig().flushWalThreshold;
        String walPath = TsfileDBDescriptor.getInstance().getConfig().walFolder;
        if (walPath.length() > 0 && walPath.charAt(walPath.length() - 1) != File.separatorChar) {
           walPath += File.separatorChar;
        }
        filePath = walPath + path + ".log";
        backFilePath = filePath + ".backup";
        plansInMemory = new ArrayList<>();
        hasBufferWriteFlush = false;
        hasOverflowFlush = false;
        logSize = 0;
    }

    void setLogCompactSize(int size) {
        this.LogCompactSize = size;
    }

    void setLogMemorySize(int size) {
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
            plansInMemory.add(new InsertPlan(2, record.deltaObjectId, record.time, measurementList, insertValues));
        } else if (flag == WriteLogManager.BUFFERWRITER) {
            List<String> measurementList = new ArrayList<>();
            List<String> insertValues = new ArrayList<>();
            for (DataPoint dp : record.dataPointList) {
                measurementList.add(dp.getMeasurementId());
                insertValues.add(dp.getValue().toString());
            }
            plansInMemory.add(new InsertPlan(1, record.deltaObjectId, record.time, measurementList, insertValues));
        }
        if (plansInMemory.size() >= LogMemorySize) {
            serializeMemoryToFile();
            logSize += plansInMemory.size();
            checkLogsCompactFileSize(false);
        }
    }

    synchronized void overflowFlushStart() throws IOException {
        serializeMemoryToFile();

        if (writer == null) {
            writer = new LocalFileLogWriter(filePath);
        }
        byte[] flushStart = new byte[1];
        flushStart[0] = (byte) SystemLogOperator.OVERFLOWFLUSHSTART;
        writer.write(flushStart);
        writer.write(BytesUtils.intToBytes(flushStart.length)); // 2 bytes to represent the content size
        LOG.info("Write overflow log start.");
    }

    synchronized void overflowFlushEnd() throws IOException {
        serializeMemoryToFile();

        if (writer == null) {
            writer = new LocalFileLogWriter(filePath);
        }
        byte[] flushEnd = new byte[1];
        flushEnd[0] = (byte) SystemLogOperator.OVERFLOWFLUSHEND;
        writer.write(flushEnd);
        writer.write(BytesUtils.intToBytes(flushEnd.length));
        hasOverflowFlush = true;
        LOG.info("Write overflow log end.");
        checkLogsCompactFileSize(false);
    }

    synchronized void bufferFlushStart() throws IOException {
        serializeMemoryToFile();

        if (writer == null) {
            writer = new LocalFileLogWriter(filePath);
        }
        byte[] flushStart = new byte[1];
        flushStart[0] = (byte) SystemLogOperator.BUFFERFLUSHSTART;
        writer.write(flushStart);
        writer.write(BytesUtils.intToBytes(flushStart.length));
        LOG.info("Write bufferwrite log start.");
    }

    synchronized void bufferFlushEnd() throws IOException {
        serializeMemoryToFile();

        if (writer == null) {
            writer = new LocalFileLogWriter(filePath);
        }
        byte[] flushEnd = new byte[1];
        flushEnd[0] = (byte) SystemLogOperator.BUFFERFLUSHEND;
        writer.write(flushEnd);
        writer.write(BytesUtils.intToBytes(flushEnd.length));
        LOG.info("Write bufferwrite log end.");
        hasBufferWriteFlush = true;
        checkLogsCompactFileSize(false);
    }

    /**
     * Compact logs in path.log.
     *
     * @throws IOException
     */
    synchronized private void checkLogsCompactFileSize(boolean forceCompact) throws IOException {
        if (logSize >= LogCompactSize && hasBufferWriteFlush ||
                (logSize >= LogCompactSize && hasOverflowFlush) || forceCompact) {
            LOG.info("Log Compact Process Begin.");
            LocalFileLogWriter writerV2 = new LocalFileLogWriter(backFilePath);
            LocalFileLogReader oldReader = new LocalFileLogReader(filePath);
            writerV2.write(oldReader.getFileCompactData());
            // Don't forget to close the stream.
            writerV2.close();
            oldReader.close();
            writer.close();
            writer = null;
            if (!new File(filePath).delete()) {
                LOG.error("Error in compact log : old log file can not delete");
                throw new IOException("Error in compact log : old log file can not delete");
            }
            if (!new File(filePath + ".backup").renameTo(new File(filePath))) {
                LOG.error("Error in compact log : can not rename to new log file");
                throw new IOException("Error in compact log : can not rename to new log file");
            }
            logSize = 0;
            hasBufferWriteFlush = false;
            hasOverflowFlush = false;
            LOG.info("Log Compact Process End.");
        }
    }

    synchronized void serializeMemoryToFile() throws IOException {
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
            LOG.info("system log compact error occured last time!");
            // need delete origin file
            if (!f.delete()) {
                LOG.error("Error in system log recovery. old .backup file could not be deleted!");
                throw new IOException("Error in system log recovery. old .backup file could not be deleted!");
            }
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


    public void closeStreams() throws IOException {
        closeReadStream();
        closeWriteStream();
    }

    public void closeWriteStream() {
        if (writer != null) {
            writer.close();
            writer = null;
        }
    }

    public void closeReadStream() throws IOException {
        if (reader != null) {
            reader.close();
            reader = null;
        }
    }

    public void readerReset() throws IOException {
        if (this.reader != null) {
            this.reader.close();
        }
        this.reader = null;
    }

    public void removeFiles() throws IOException {
        File currentFile = new File(filePath);
        //currentFile.
        if (currentFile.exists()) {
            if (!currentFile.delete() ) {
                LOG.error("current file can not delete");
                throw new IOException("current file can not delete");
            }
        }

        File backFile = new File(backFilePath);
        if (backFile.exists()) {
            if (!backFile.delete() ) {
                LOG.error("backup file can not delete");
                throw new IOException("backup file can not delete");
            }
        }
    }
}
