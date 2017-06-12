package cn.edu.thu.tsfiledb.sys.writeLog;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import cn.edu.thu.tsfile.timeseries.write.record.TSRecord;
import cn.edu.thu.tsfiledb.conf.TsfileDBDescriptor;
import cn.edu.thu.tsfiledb.exception.PathErrorException;
import cn.edu.thu.tsfiledb.metadata.MManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfiledb.qp.physical.PhysicalPlan;

public class WriteLogManager {
    private static final Logger LOG = LoggerFactory.getLogger(WriteLogManager.class);
    private static WriteLogManager instance;
    private static HashMap<String, WriteLogNode> logNodeMaps;
    public static final int BUFFERWRITER = 0, OVERFLOW = 1;
    private static List<String> recoveryPathList = new ArrayList<>();
    public static boolean isRecovering = false;
    
    private WriteLogManager() {
        logNodeMaps = new HashMap<>();
    }

    public static WriteLogManager getInstance() {
        if (instance == null) {
            synchronized (WriteLogManager.class) {
                instance = new WriteLogManager();
            }
        }
        return instance;
    }

    public static WriteLogNode getWriteLogNode(String fileNode) {
        if (logNodeMaps.containsKey(fileNode)) {
            return logNodeMaps.get(fileNode);
        }
        logNodeMaps.put(fileNode, new WriteLogNode(fileNode));
        return logNodeMaps.get(fileNode);
    }

    public void write(PhysicalPlan plan) throws IOException, PathErrorException {
        getWriteLogNode(MManager.getInstance().getFileNameByPath(plan.getPaths().get(0).getFullPath())).write(plan);
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
                String filePath = TsfileDBDescriptor.getInstance().getConfig().walFolder + iterator.next() + ".log";
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
}
