package cn.edu.tsinghua.iotdb.writelog.manager;

import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.exception.RecoverException;
import cn.edu.tsinghua.iotdb.writelog.node.ExclusiveWriteLogNode;
import cn.edu.tsinghua.iotdb.writelog.node.WriteLogNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

public class MultiFileLogNodeManager implements WriteLogNodeManager {

    private static final Logger logger = LoggerFactory.getLogger(MultiFileLogNodeManager.class);
    private Map<String, WriteLogNode> nodeMap;

    private Thread syncThread;
    private final String syncThreadName = "IoTDB-MultiFileLogNodeManager-Sync-Thread";
    private TsfileDBConfig config = TsfileDBDescriptor.getInstance().getConfig();

    private static class InstanceHolder {
        private static MultiFileLogNodeManager instance = new MultiFileLogNodeManager();
    }

    private final Runnable syncTask = new Runnable() {
        @Override
        public void run() {
            while(true) {
                if(Thread.interrupted()){
                    logger.info("WAL sync thread exits.");
                    break;
                }
                logger.debug("Timed sync starts, {} nodes to be flushed", nodeMap.size());
                for(WriteLogNode node : nodeMap.values()) {
                    try {
                        node.forceSync();
                    } catch (IOException e) {
                        logger.error("Cannot sync {}, because {}", node.toString(), e.toString());
                    }
                }
                logger.debug("Timed sync finished");
                try {
                    Thread.sleep(config.flushWalPeriodInMs);
                } catch (InterruptedException e) {
                    logger.info("WAL sync thread exits.");
                    break;
                }
            }
        }
    };

    private MultiFileLogNodeManager() {
        nodeMap = new ConcurrentHashMap<>();
        syncThread = new Thread(syncTask, syncThreadName);
        syncThread.start();
    }

    static public MultiFileLogNodeManager getInstance() {
        if(!InstanceHolder.instance.syncThread.isAlive()) {
            synchronized (logger) {
                InstanceHolder.instance.syncThread = new Thread(InstanceHolder.instance.syncTask, "IoTDB-MultiFileLogNodeManager-Sync-Thread");
                InstanceHolder.instance.syncThread.start();
            }
        }
        return InstanceHolder.instance;
    }

    @Override
    public WriteLogNode getNode(String identifier, String restoreFilePath, String processorStoreFilePath) throws IOException {
        WriteLogNode node = nodeMap.get(identifier);
        if(node == null && restoreFilePath != null && processorStoreFilePath != null) {
            node = new ExclusiveWriteLogNode(identifier, restoreFilePath, processorStoreFilePath);
            WriteLogNode oldNode = nodeMap.putIfAbsent(identifier, node);
            if(oldNode != null)
                return oldNode;
        }
        return node;
    }

    @Override
    public void deleteNode(String identifier) throws IOException {
        WriteLogNode node = nodeMap.remove(identifier);
        if(node != null) {
            node.delete();
        }
    }

    /*
    Warning : caller must guarantee thread safety.
     */
    @Override
    public void recover() throws RecoverException {
        List<WriteLogNode> nodeList = new ArrayList<>(nodeMap.size());
        nodeList.addAll(nodeMap.values());
        nodeList.sort(null);
        for(WriteLogNode node : nodeList) {
            try {
                node.recover();
            } catch (RecoverException e) {
                logger.error("{} failed to recover because {}", node.toString(), e.getMessage());
                throw e;
            }
        }
    }

    @Override
    public void close() {
        logger.info("LogNodeManager starts closing..");
        syncThread.interrupt();
        logger.info("Waiting for sync thread to stop");
        while(syncThread.isAlive()) {
            // wait
        }
        logger.info("{} nodes to be closed", nodeMap.size());
        for(WriteLogNode node : nodeMap.values()) {
            try {
                node.close();
            } catch (IOException e) {
                logger.error("{} failed to close because {}", node.toString(), e.getMessage());
            }
        }
        nodeMap.clear();
        logger.info("LogNodeManager closed.");
    }

}
