package org.apache.iotdb.db.engine.alter;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.engine.alter.log.AlteringLogAnalyzer;
import org.apache.iotdb.db.engine.cache.AlteringRecordsCache;
import org.apache.iotdb.db.engine.compaction.log.TsFileIdentifier;
import org.apache.iotdb.db.engine.compaction.task.AbstractCompactionTask;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileNameGenerator;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceList;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class RewriteTImeseriesTask extends AbstractCompactionTask {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);

    private final List<TsFileResource> candidateResourceList = new ArrayList<>(32);

    private final List<TsFileResource> readyResourceList = new ArrayList<>(32);

    private AlteringRecordsCache alteringRecordsCache = AlteringRecordsCache.getInstance();

    private boolean isClearBegin = false;

    private final String logKey;

    public RewriteTImeseriesTask(String storageGroupName, String dataRegionId, long timePartition, TsFileManager tsFileManager, AtomicInteger currentTaskNum, long serialId, boolean isClearBegin, Set<TsFileIdentifier> doneFiles) {

        super(storageGroupName, dataRegionId, timePartition, tsFileManager, currentTaskNum, serialId);
        this.logKey = storageGroupName+dataRegionId+timePartition;
        this.isClearBegin = isClearBegin;
        // get seq tsFile list
        TsFileResourceList seqTsFileResourcList = tsFileManager.getSequenceListByTimePartition(timePartition);
        // Gets the device list from the cache
        Set<String> devicesCache = alteringRecordsCache.getDevicesCache(storageGroupName);
        if(seqTsFileResourcList != null && seqTsFileResourcList.size() > 0) {
            seqTsFileResourcList.forEach(tsFileResource -> {
                Set<String> devices = tsFileResource.getDevices();
                if(devices == null) {
                    return;
                }
                // AlteringLog filter
                if(isClearBegin && doneFiles != null && doneFiles.size() > 0) {
                    for (TsFileIdentifier tsFileIdentifier :
                            doneFiles) {
                        try {
                            TsFileNameGenerator.TsFileName logTsFileName = TsFileNameGenerator.getTsFileName(tsFileIdentifier.getFilename());
                            TsFileNameGenerator.TsFileName tsFileName = TsFileNameGenerator.getTsFileName(tsFileResource.getTsFile().getName());
                            // As long as time and version are the same, they are considered to be the same file
                            if(logTsFileName.getTime() == tsFileName.getTime() && logTsFileName.getVersion() == tsFileName.getVersion()) {
                                LOGGER.info("[rewriteTimeseries] {} the file {} has been done", logKey, tsFileResource.getTsFilePath());
                                return;
                            }
                        } catch (IOException e) {
                            LOGGER.warn("tsfile name parseFailed");
                            return;
                        }
                    }
                }
                // device filter
                // In most scenarios, the number of devices in the tsfile file is greater than the number of devices to be modified
                for (String device: devicesCache) {
                    // Looking for intersection
                    // TODO Use a better algorithm instead
                    if(devices.contains(device)) {
                       candidateResourceList.add(tsFileResource);
                       break;
                    }
                }
            });
        }
    }

    @Override
    public void setSourceFilesToCompactionCandidate() {

        while (candidateResourceList.size() > 0) {
            List<TsFileResource> removeList = new ArrayList<>(candidateResourceList.size());
            candidateResourceList.forEach(x -> {
                if(x.isDeleted() || !x.isClosed()) {
                    removeList.add(x);
                    return;
                }
                if(x.getStatus() == TsFileResourceStatus.CLOSED) {
                    try {
                        x.setStatus(TsFileResourceStatus.COMPACTION_CANDIDATE);
                        readyResourceList.add(x);
                        removeList.add(x);
                    } catch (Exception e) {
                        // do nothing
                    }
                }
            });
            candidateResourceList.removeAll(removeList);
        }
    }

    @Override
    protected void doCompaction() {

    }

    @Override
    public boolean equalsOtherTask(AbstractCompactionTask otherTask) {
        if(!(otherTask instanceof RewriteTImeseriesTask)) {
            return false;
        }
        RewriteTImeseriesTask task = (RewriteTImeseriesTask) otherTask;
        return task.getStorageGroupName().equals(this.getStorageGroupName()) && this.getDataRegionId().equals(this.getDataRegionId()) && this.getTimePartition() == this.getTimePartition();
    }

    @Override
    public boolean checkValidAndSetMerging() {
        return false;
    }

    @Override
    public void resetCompactionCandidateStatusForAllSourceFiles() {

    }

}
