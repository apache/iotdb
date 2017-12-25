package cn.edu.tsinghua.iotdb.sys.writelog;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.metadata.MManager;
//import org.slf4j.LoggerFactory;
import cn.edu.tsinghua.iotdb.qp.physical.PhysicalPlan;
import cn.edu.tsinghua.iotdb.utils.IoTDBThreadPoolFactory;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriteLogManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(WriteLogManager.class);
    // to determine whether system is in recovering process
    public static boolean isRecovering = false;
    // 0 represents BUFFERWRITE insert operation, 1 represents OVERFLOW insert operation
    public static final int BUFFERWRITER = 0, OVERFLOW = 1;
    private static List<String> recoveryPathList = new ArrayList<>();
    
    private static class WriteLogManagerHolder {  
        private static final WriteLogManager INSTANCE = new WriteLogManager();  
    }
    private static ConcurrentHashMap<String, WriteLogNode> logNodeMaps = new ConcurrentHashMap<>();

    /** timing thread to execute wal task periodically */
    private ScheduledExecutorService timingService;

    private WriteLogManager() {
        if (TsfileDBDescriptor.getInstance().getConfig().enableWal) {
            logNodeMaps = new ConcurrentHashMap<>();
            // system log timing merge task
            timingService = IoTDBThreadPoolFactory.newScheduledThreadPool(1,"WALFlush");
            long delay = 0;
            long interval = TsfileDBDescriptor.getInstance().getConfig().flushWalPeriodInMs;
            timingService.scheduleAtFixedRate(new LogMergeTimingTask(), delay, interval, TimeUnit.SECONDS);
        }
    }

    class LogMergeTimingTask implements Runnable {
        public void run() {
            try {
                for (Map.Entry<String, WriteLogNode> entry : logNodeMaps.entrySet()) {
                    entry.getValue().serializeMemoryToFile();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static final WriteLogManager getInstance() {
        return WriteLogManagerHolder.INSTANCE;
    }

    private static WriteLogNode getWriteLogNode(String fileNode) {
        if (logNodeMaps.containsKey(fileNode)) {
            return logNodeMaps.get(fileNode);
        }
        logNodeMaps.put(fileNode, new WriteLogNode(fileNode));
        return logNodeMaps.get(fileNode);
    }

    /**
     * Write the content of PhysicalPlan into WAL system.
     * Note that this method is only write UPDATE/DELETE operation.
     *
     * @param filenodeName the name of filenode for the plan
     * @param plan PhysicalPlan to serialize
     * @throws IOException  write WAL file error
     * @throws PathErrorException serialize <code>Path</code> error
     */
    public void write(String filenodeName, PhysicalPlan plan) throws IOException, PathErrorException {
		getWriteLogNode(filenodeName).write(plan);
    }

    /**
     * Write the content of TSRecord into WAL system.
     * Note that this method is only write INSERT operation.
     * 
     * @param filenodeName the name of filenode for the record
     * @param record <code>TSRecord</code>
     * @param type determines the INSERT operation is Overflow or Bufferwrite
     * @throws IOException write WAL file error
     * @throws PathErrorException serialize <code>Path</code> error
     */
    public void write(String filenodeName, TSRecord record, int type) throws IOException, PathErrorException {
        getWriteLogNode(filenodeName).write(record, type);
    }

    public void startOverflowFlush(String nsPath) throws IOException {
        getWriteLogNode(nsPath).overflowFlushStart();
    }

    public void endOverflowFlush(String nsPath) throws IOException {
        getWriteLogNode(nsPath).overflowFlushEnd();
    }

    public void startBufferWriteFlush(String nsPath) throws IOException {
        getWriteLogNode(nsPath).bufferFlushStart();
    }

    public void endBufferWriteFlush(String nsPath) throws IOException {
        getWriteLogNode(nsPath).bufferFlushEnd();
    }

    public void recovery() throws IOException {
        try {
            //TODO need optimize
            recoveryPathList = MManager.getInstance().getAllFileNames();
            Iterator<String> iterator = recoveryPathList.iterator();
            while (iterator.hasNext()) {
                String walPath = TsfileDBDescriptor.getInstance().getConfig().walFolder;
                if (walPath.length() > 0 && walPath.charAt(walPath.length() - 1) != File.separatorChar) {
                    walPath += File.separatorChar;
                }
                String filePath = walPath + iterator.next() + ".log";
                if (!new File(filePath).exists()) {
                    iterator.remove();
                }
            }
        } catch (PathErrorException e) {
            throw new IOException(e);
        }
    }

    public PhysicalPlan getPhysicalPlan() throws IOException {
        if (recoveryPathList.size() == 0)
            return null;

        Iterator<String> iterator = recoveryPathList.iterator();
        while (iterator.hasNext()) {
            WriteLogNode node = getWriteLogNode(iterator.next());
            node.recovery();
            PhysicalPlan plan = node.getPhysicalPlan();
            if (plan != null) {
                return plan;
            } else {
            	node.closeReadStream();
                iterator.remove();
            }
        }
        return null;
    }

    /**
     * Close the file streams of WAL and shutdown the timing thread.
     *
     * @throws IOException file close error
     */
    public void close() throws IOException {
        for (Map.Entry<String, WriteLogNode> entry : logNodeMaps.entrySet()) {
            entry.getValue().closeStreams();
        }
        if(timingService.isShutdown()){
        	return;
        }
        timingService.shutdown();
        
        try {
            timingService.awaitTermination(10, TimeUnit.SECONDS);
            LOGGER.info("shutdown wal server successfully");
        } catch (InterruptedException e) {
            LOGGER.error("wal manager timing service could not be shutdown");
            // e.printStackTrace();
        }
    }
}
