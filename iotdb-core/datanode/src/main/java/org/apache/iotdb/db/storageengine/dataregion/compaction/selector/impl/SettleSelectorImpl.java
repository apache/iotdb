package org.apache.iotdb.db.storageengine.dataregion.compaction.selector.impl;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.CommonDateTimeUtils;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.DataNodeTTLCache;
import org.apache.iotdb.db.storageengine.dataregion.compaction.constant.CompactionTaskPriorityType;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.FastCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.AbstractCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.InnerSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.SettleCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.ISettleSelector;
import org.apache.iotdb.db.storageengine.dataregion.modification.Deletion;
import org.apache.iotdb.db.storageengine.dataregion.modification.Modification;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.DeviceTimeIndex;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.ITimeIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SettleSelectorImpl implements ISettleSelector {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);
    private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    private static final float dirtyDataRate = config.getExpiredDataRate();
    private static final long longestOutdatedTime = config.getLongestExpiredTime();

    boolean heavySelect;

    protected String storageGroupName;
    protected String dataRegionId;
    protected long timePartition;
    protected TsFileManager tsFileManager;

    public SettleSelectorImpl(boolean heavySelect, String storageGroupName, String dataRegionId, long timePartition, TsFileManager tsFileManager) {
        this.heavySelect = heavySelect;
        this.storageGroupName = storageGroupName;
        this.dataRegionId = dataRegionId;
        this.timePartition = timePartition;
        this.tsFileManager = tsFileManager;
    }

    static class AllDirtyResource{
        List<TsFileResource> resources = new ArrayList<>();
        public void add(TsFileResource resource){
            resources.add(resource);
        }

        public List<TsFileResource> getResources(){
            return resources;
        }
    }

    static class PartialDirtyResource{
        List<TsFileResource> resourcesForSettleTask = new ArrayList<>();
        List<List<TsFileResource>> resourcesForInnerTasks = new ArrayList<>();

        List<TsFileResource> tmpResources = new ArrayList<>();
        long totalTmpFileSize = 0;

        public void add(TsFileResource resource){
            tmpResources.add(resource);
            totalTmpFileSize+= resource.getTsFileSize();
            if(tmpResources.size()>= config.getFileLimitPerInnerTask() || totalTmpFileSize >= config.getTargetCompactionFileSize()){
                submitTmpResources();
            }
        }

        public void submitTmpResources(){
            if(tmpResources.isEmpty()){
                return;
            }
            if(tmpResources.size()==1){
                resourcesForSettleTask.add(tmpResources.get(0));
                tmpResources.clear();
            }else{
                resourcesForInnerTasks.add(tmpResources);
                tmpResources = new ArrayList<>();
            }
            totalTmpFileSize = 0;
        }

        public List<TsFileResource> getResourcesForSettleTask() {
            return resourcesForSettleTask;
        }

        public List<List<TsFileResource>> getResourcesForInnerTasks() {
            return resourcesForInnerTasks;
        }
    }


    @Override
    public List<AbstractCompactionTask> selectSettleTask(List<TsFileResource> seqFiles, List<TsFileResource> unseqFiles) {
        List<AbstractCompactionTask> tasks = new ArrayList<>();
        tasks.addAll(selectTasks(seqFiles));
        tasks.addAll(selectTasks(unseqFiles));
        return tasks;
    }

    private List<AbstractCompactionTask> selectTasks(List<TsFileResource> resources){
        AllDirtyResource allDirtyResource= new AllDirtyResource();
        PartialDirtyResource partialDirtyResource = new PartialDirtyResource();
        try {
            for (TsFileResource resource : resources) {
                if (resource.getStatus() != TsFileResourceStatus.NORMAL) {
                    continue;
                }

                DirtyStatus dirtyStatus;
                if (!heavySelect) {
                    dirtyStatus = selectFileBaseOnModSize(resource);
                } else {
                    dirtyStatus = selectFileBaseOnDirtyData(resource);
                    if (dirtyStatus == DirtyStatus.NOT_SATISFIED) {
                        dirtyStatus = selectFileBaseOnModSize(resource);
                    }
                }

                switch (dirtyStatus) {
                    case ALL_DELETED:
                        allDirtyResource.add(resource);
                        break;
                    case PARTIAL_DELETED:
                        partialDirtyResource.add(resource);
                        break;
                    case NOT_SATISFIED:
                        partialDirtyResource.submitTmpResources();
                        break;
                    default:
                        // do nothing
                }
            }
            return createTask(allDirtyResource, partialDirtyResource);
        } catch (Exception e) {
            LOGGER.error("{}-{} cannot select file for settle compaction", storageGroupName,dataRegionId, e);
        }
        return Collections.emptyList();
    }

    private DirtyStatus selectFileBaseOnModSize(TsFileResource resource){
        ModificationFile modFile = resource.getModFile();
        if(modFile == null || !modFile.exists()){
            return DirtyStatus.NOT_SATISFIED;
        }
        return modFile.getSize() > IoTDBDescriptor.getInstance().getConfig().getInnerCompactionTaskSelectionModsFileThreshold()?
                DirtyStatus.PARTIAL_DELETED:DirtyStatus.NOT_SATISFIED;
    }

    /**
     * Only when all devices with ttl are deleted may they be selected. On the basic of the previous, only when the number of deleted devices exceeds the threshold or has expired for too long will they be selected.
     * @return dirty status means the status of current resource.
     */
    private DirtyStatus selectFileBaseOnDirtyData(TsFileResource resource) throws IOException, IllegalPathException {
        ModificationFile modFile = resource.getModFile();
        DeviceTimeIndex deviceTimeIndex = resource.getTimeIndexType() == ITimeIndex.FILE_TIME_INDEX_TYPE?resource.buildDeviceTimeIndex(): (DeviceTimeIndex) resource.getTimeIndex();
        Set<String> deletedDevices = new HashSet<>();
        boolean hasExpiredTooLong = false;
        long currentTime = CommonDateTimeUtils.currentTime();

        Collection<Modification> modifications = modFile.getModifications();
        for(String device: deviceTimeIndex.getDevices()){
            // check expired device by ttl
            long deviceTTL = DataNodeTTLCache.getInstance().getTTL(device);
            boolean hasSetTTL = deviceTTL!= Long.MAX_VALUE;
            boolean isDeleted = !deviceTimeIndex.isDeviceAlive(device,deviceTTL);
            if(!isDeleted){
                // check deleted device by mods
                isDeleted = isDeviceDeletedByMods(modifications,device,deviceTimeIndex.getStartTime(device),deviceTimeIndex.getEndTime(device));
            }
            if(hasSetTTL){
                if(!isDeleted) {
                    return DirtyStatus.NOT_SATISFIED;
                }
                long outdatedTimeDiff = currentTime - deviceTimeIndex.getEndTime(device);
                hasExpiredTooLong = hasExpiredTooLong || outdatedTimeDiff > Math.min(longestOutdatedTime,3*deviceTTL);
            }

            if(isDeleted) {
                deletedDevices.add(device);
            }
        }

        float deletedDeviceRate = (float)(deletedDevices.size()) / deviceTimeIndex.getDevices().size();
        if(deletedDeviceRate == 1f){
            // the whole file is completely dirty
            return DirtyStatus.ALL_DELETED;
        }
        if(hasExpiredTooLong || deletedDeviceRate >= dirtyDataRate){
            return DirtyStatus.PARTIAL_DELETED;
        }
        return DirtyStatus.NOT_SATISFIED;
    }

    /**
     * Check whether the device is completely deleted by mods or not.
     */
    private boolean isDeviceDeletedByMods(Collection<Modification> modifications, String device, long startTime, long endTime) throws IllegalPathException {
        for(Modification modification:modifications){
            boolean isPathMatched = false;
            for(PartialPath modDevicePath:modification.getPath().getDevicePathPattern()){
                if(modDevicePath.matchFullPath(new PartialPath(device))){
                    isPathMatched = true;
                    break;
                }
            }
            if(isPathMatched &&
                    ((Deletion)modification).getTimeRange().contains(startTime,endTime)){
                return true;
            }
        }
        return false;
    }

    private List<AbstractCompactionTask> createTask(AllDirtyResource allDirtyResource, PartialDirtyResource partialDirtyResource){
        List<AbstractCompactionTask> tasks = new ArrayList<>();
        // create settle compaction task
        if(!allDirtyResource.getResources().isEmpty() || !partialDirtyResource.getResourcesForSettleTask().isEmpty()){
            tasks.add(new SettleCompactionTask(timePartition,tsFileManager,allDirtyResource.getResources(),partialDirtyResource.getResourcesForSettleTask(),
                    new FastCompactionPerformer(false),tsFileManager.getNextCompactionTaskId()));
        }

        // create inner compaction task
        for(List<TsFileResource> tsFileResources:partialDirtyResource.getResourcesForInnerTasks()){
            if(tsFileResources.isEmpty()){
                continue;
            }
            tasks.add(
                    new InnerSpaceCompactionTask(timePartition,tsFileManager,tsFileResources,tsFileResources.get(0).isSeq(),
                            new FastCompactionPerformer(false),tsFileManager.getNextCompactionTaskId(), CompactionTaskPriorityType.MOD_SETTLE));
        }
        return tasks;
    }

    enum DirtyStatus{
        ALL_DELETED,        // the whole file is deleted
        PARTIAL_DELETED,    // the file is partial deleted
        NOT_SATISFIED       // do not satisfy settle condition, which does not mean there is no dirty data
    }


}
