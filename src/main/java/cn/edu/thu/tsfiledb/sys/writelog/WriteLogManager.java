package cn.edu.thu.tsfiledb.sys.writelog;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import cn.edu.thu.tsfile.timeseries.write.record.TSRecord;
import cn.edu.thu.tsfiledb.conf.TsfileDBDescriptor;
import cn.edu.thu.tsfiledb.exception.PathErrorException;
import cn.edu.thu.tsfiledb.metadata.MManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfiledb.qp.physical.PhysicalPlan;

public class WriteLogManager {
    private static final Logger LOG = LoggerFactory.getLogger(WriteLogManager.class);
    private static WriteLogManager instance = new WriteLogManager();
    private static ConcurrentHashMap<String, WriteLogNode> logNodeMaps;
    public static final int BUFFERWRITER = 0, OVERFLOW = 1;
    private static List<String> recoveryPathList = new ArrayList<>();
    public static boolean isRecovering = false;

    private WriteLogManager() {
        if (TsfileDBDescriptor.getInstance().getConfig().enableWal) {
            logNodeMaps = new ConcurrentHashMap<>();
            // system log timing merge task
            ScheduledExecutorService service = Executors.newScheduledThreadPool(1);
            long delay = 0;
            long interval = TsfileDBDescriptor.getInstance().getConfig().flushWalPeriodInMs;
            service.scheduleAtFixedRate(new LogMergeTimingTask(), delay, interval, TimeUnit.SECONDS);
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

    public static WriteLogManager getInstance() {
        return instance;
    }

    private static WriteLogNode getWriteLogNode(String fileNode) {
        if (logNodeMaps.containsKey(fileNode)) {
            return logNodeMaps.get(fileNode);
        }
        logNodeMaps.put(fileNode, new WriteLogNode(fileNode));
        return logNodeMaps.get(fileNode);
    }

    public void write(PhysicalPlan plan) throws IOException, PathErrorException {
    		for(int i = 0;i < plan.getPaths().size(); i++){
    			getWriteLogNode(MManager.getInstance().getFileNameByPath(plan.getPaths().get(i).getFullPath())).write(plan);
    		}
    }

    public void write(TSRecord record, int type) throws IOException, PathErrorException {
        getWriteLogNode(MManager.getInstance().getFileNameByPath(record.deltaObjectId)).write(record, type);
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
                iterator.remove();
            }
        }
        return null;
    }

    public void close() throws IOException {
        for (Map.Entry<String, WriteLogNode> entry : logNodeMaps.entrySet()) {
            entry.getValue().closeStreams();
        }
    }
}
