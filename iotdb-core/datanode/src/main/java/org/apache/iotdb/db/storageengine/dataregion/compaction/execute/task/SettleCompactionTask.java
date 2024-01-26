package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.service.metrics.CompactionMetrics;
import org.apache.iotdb.db.service.metrics.FileMetrics;
import org.apache.iotdb.db.storageengine.dataregion.compaction.constant.CompactionTaskPriorityType;
import org.apache.iotdb.db.storageengine.dataregion.compaction.constant.CompactionTaskType;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.exception.CompactionRecoverException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.ICompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.FastCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.subtask.FastCompactionTaskSummary;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.CompactionUtils;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.CompactionLogAnalyzer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.CompactionLogger;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.SimpleCompactionLogger;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.TsFileIdentifier;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.estimator.AbstractInnerSpaceEstimator;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.estimator.FastCompactionInnerCompactionEstimator;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.generator.TsFileNameGenerator;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.utils.TsFileUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SettleCompactionTask extends AbstractCompactionTask{
    private List<TsFileResource> allDeletedFiles;
    private List<TsFileResource> partialDeletedFiles;

    /** Only partial deleted files have corresponding target files. */
    private List<TsFileResource> targetFiles;

    private File logFile;

    private double allDeletedFileSize = 0;
    private double partialDeletedFileSize = 0;

    private AbstractInnerSpaceEstimator spaceEstimator;

    public SettleCompactionTask(
            long timePartition,
            TsFileManager tsFileManager,
            List<TsFileResource> allDeletedFiles,
            List<TsFileResource> partialDeletedFiles,
            ICompactionPerformer performer,
            long serialId) {
        super(
                tsFileManager.getStorageGroupName(),
                tsFileManager.getDataRegionId(),
                timePartition,
                tsFileManager,
                serialId,
                CompactionTaskPriorityType.MOD_SETTLE);
        this.allDeletedFiles = allDeletedFiles;
        this.partialDeletedFiles = partialDeletedFiles;
        this.performer = performer;
        if (IoTDBDescriptor.getInstance().getConfig().isEnableCompactionMemControl()) {
            spaceEstimator = new FastCompactionInnerCompactionEstimator();
        }
        this.hashCode = this.toString().hashCode();
        createSummary();
    }

    public SettleCompactionTask(
            String databaseName, String dataRegionId, TsFileManager tsFileManager, File logFile) {
        super(databaseName, dataRegionId, 0L, tsFileManager, 0L, CompactionTaskPriorityType.MOD_SETTLE);
        this.logFile = logFile;
        this.needRecoverTaskInfoFromLogFile = true;
    }

    @Override
    public List<TsFileResource> getAllSourceTsFiles() {
        List<TsFileResource> allSourceFiles = new ArrayList<>(allDeletedFiles);
        allSourceFiles.addAll(partialDeletedFiles);
        return allSourceFiles;
    }

    @Override
    protected boolean doCompaction() {
        recoverMemoryStatus = true;
        boolean isSuccess = true;
        try{
            if (!tsFileManager.isAllowCompaction()) {
                return true;
            }
            if(allDeletedFiles.isEmpty() || partialDeletedFiles.isEmpty()){
                LOGGER.info(
                        "{}-{} [Compaction] Settle compaction file list is empty, end it",
                        storageGroupName,
                        dataRegionId);
            }
            long startTime = System.currentTimeMillis();
            allDeletedFiles.forEach(x -> allDeletedFileSize+=x.getTsFileSize());
            partialDeletedFiles.forEach(x -> partialDeletedFileSize+=x.getTsFileSize());

            targetFiles = TsFileNameGenerator.getSettleCompactionTargetFileResources(partialDeletedFiles);

            LOGGER.info(
                    "{}-{} [Compaction] SettleCompaction task starts with {} all_deleted files "
                            + "and {} partial_deleted files. "
                            + "All_deleted files : {}, partial_deleted files : {} . "
                            + "All_deleted files size is {} MB, "
                            + "partial_deleted file size is {} MB. ",
                    storageGroupName,
                    dataRegionId,
                    allDeletedFiles.size(),
                    partialDeletedFiles.size(),
                    allDeletedFiles,
                    partialDeletedFiles,
                    allDeletedFileSize / 1024 / 1024,
                    partialDeletedFileSize / 1024 / 1024);

            List<TsFileResource> allSourceFiles = getAllSourceTsFiles();
            logFile = new File(allSourceFiles.get(0).getTsFile().getAbsolutePath() + CompactionLogger.SETTLE_COMPACTION_LOG_NAME_SUFFIX);
            try (SimpleCompactionLogger compactionLogger = new SimpleCompactionLogger(logFile)) {
                compactionLogger.logSourceFiles(allSourceFiles);
                compactionLogger.logTargetFiles(targetFiles);
                compactionLogger.logEmptyTargetFiles(allDeletedFiles);
                compactionLogger.force();

                settleWithAllDeletedFile();
                settleWithPartialDeletedFile(compactionLogger);
            }

            CompactionMetrics.getInstance().recordSummaryInfo(summary);
            double costTime = (System.currentTimeMillis() - startTime) / 1000.0d;

            LOGGER.info(
                    "{}-{} [Compaction] SettleCompaction task finishes successfully, "
                            + "time cost is {} s, "
                            + "compaction speed is {} MB/s, {}",
                    storageGroupName,
                    dataRegionId,
                    String.format("%.2f", costTime),
                    String.format(
                            "%.2f",
                            (allDeletedFileSize + partialDeletedFileSize) / 1024.0d / 1024.0d / costTime),
                    summary);


        }catch (Exception e){
            isSuccess = false;
            printLogWhenException(LOGGER, e);
            recover();
        }
        return isSuccess;
    }

    private void settleWithAllDeletedFile() {
        allDeletedFiles.forEach(this::deleteTsFileOnDisk);
    }

    private void settleWithPartialDeletedFile(SimpleCompactionLogger compactionLogger) throws Exception {
        for(int i=0;i<partialDeletedFiles.size();i++){
            boolean isSequence = partialDeletedFiles.get(i).isSeq();
            TsFileResource targetResource = targetFiles.get(i);
            List<TsFileResource> singleSourceList = Collections.singletonList(partialDeletedFiles.get(i));
            List<TsFileResource> singleTargetList = Collections.singletonList(targetResource);
            performer.setSourceFiles(singleSourceList);
            performer.setTargetFiles(singleTargetList);
            performer.setSummary(summary);
            performer.perform();

            CompactionUtils.updateProgressIndex(singleTargetList,singleSourceList,Collections.emptyList());
            CompactionUtils.moveTargetFile(singleTargetList, true, storageGroupName + "-" + dataRegionId);
            CompactionUtils.combineModsInInnerCompaction(singleSourceList,targetResource);

            validateCompactionResult(
                    isSequence?singleSourceList:Collections.emptyList(),
                    isSequence?Collections.emptyList():singleSourceList,
                    singleTargetList
            );

            // replace tsfile resource in memory
            tsFileManager.replace(
                    isSequence?singleSourceList:Collections.emptyList(),
                    isSequence?Collections.emptyList():singleSourceList,
                    singleTargetList,
                    timePartition,
                    isSequence
            );

            try{
                targetResource.writeLock();
                CompactionUtils.deleteSourceTsFileAndUpdateFileMetrics(singleSourceList,isSequence);

                if(!targetResource.isDeleted()){
                    FileMetrics.getInstance()
                            .addTsFile(
                                    targetResource.getDatabaseName(),
                                    targetResource.getDataRegionId(),
                                    targetResource.getTsFileSize(),
                                    isSequence,
                                    targetResource.getTsFile().getName());
                }else{
                    // target resource is empty after compaction, then add log and delete it
                    compactionLogger.logEmptyTargetFile(targetResource);
                    compactionLogger.force();
                    targetResource.remove();
                }
            }finally {
                targetResource.writeUnlock();
            }
        }
    }

    @Override
    protected void recover() {
        try {
            if (needRecoverTaskInfoFromLogFile) {
                recoverTaskInfoFromLogFile();
            }
            recoverAllDeletedFiles();
            recoverPartialDeletedFiles();
        } catch (Exception e) {
            handleRecoverException(e);
        }
    }

    private void recoverAllDeletedFiles(){
        for(TsFileResource resource: allDeletedFiles){
            if(checkRelatedFileExists(resource) && !deleteTsFileOnDisk(resource)){
                throw new CompactionRecoverException(
                        String.format("failed to delete all_deleted source file %s", resource));
            }
        }
    }

    private void recoverPartialDeletedFiles() throws IOException {
        for(int i=0;i<partialDeletedFiles.size();i++){
            TsFileResource resource = partialDeletedFiles.get(i);
            TsFileResource targetResource = targetFiles.get(i);
            if(resource.tsFileExists()){
                // source file exists, then roll back
                rollback(resource,targetResource);
            } else{
                // source file lost, then finish task
                finishTask(resource,targetResource);
            }
        }
    }

    private void rollback(TsFileResource sourceResource, TsFileResource targetResource) throws IOException {
        deleteCompactionModsFile(Collections.singletonList(sourceResource));
        if(targetResource == null || !targetResource.tsFileExists()){
            return;
        }
        if (recoverMemoryStatus) {
            replaceTsFileInMemory(
                    Collections.singletonList(targetResource), Collections.singletonList(sourceResource));
        }
        // delete target file
        if (!deleteTsFileOnDisk(targetResource)) {
            throw new CompactionRecoverException(
                    String.format("failed to delete target file %s", targetResource));
        }
    }

    private void finishTask(TsFileResource sourceResource, TsFileResource targetResource) throws IOException {
        if(targetResource.isDeleted()){
            // it means the target file is empty after compaction
            if (targetResource.remove()) {
                throw new CompactionRecoverException(
                        String.format("failed to delete empty target file %s", targetResource));
            }
        }else{
            File targetFile = targetResource.getTsFile();
            if (targetFile == null || !TsFileUtils.isTsFileComplete(targetResource.getTsFile())) {
                throw new CompactionRecoverException(
                        String.format("Target file is not completed. %s", targetFile));
            }
            if (recoverMemoryStatus) {
                targetResource.setStatus(TsFileResourceStatus.NORMAL);
            }
        }
        if (!deleteTsFileOnDisk(sourceResource)) {
            throw new CompactionRecoverException("source files cannot be deleted successfully");
        }
        if (recoverMemoryStatus) {
            FileMetrics.getInstance().deleteTsFile(true, Collections.singletonList(sourceResource));
        }
    }

    private boolean checkRelatedFileExists(TsFileResource resource){
        return resource.tsFileExists() || resource.resourceFileExists() || resource.getModFile().exists() || resource.getCompactionModFile().exists() ;
    }

    private void recoverTaskInfoFromLogFile() throws IOException {
        CompactionLogAnalyzer logAnalyzer = new CompactionLogAnalyzer(this.logFile);
        logAnalyzer.analyze();
        List<TsFileIdentifier> sourceFileIdentifiers = logAnalyzer.getSourceFileInfos();
        List<TsFileIdentifier> targetFileIdentifiers = logAnalyzer.getTargetFileInfos();
        List<TsFileIdentifier> deletedTargetFileIdentifiers = logAnalyzer.getDeletedTargetFileInfos();

        // target version set, it is used to distinguish all_deleted files and partial_deleted files in the log
        Set<Long> targetVersionSet = new HashSet<>();
        for(TsFileIdentifier identifier:targetFileIdentifiers){
            targetVersionSet.add(TsFileNameGenerator.getTsFileName(identifier.getFilename()).getVersion());
        }

        // recover source files, including all_deleted files and partial_deleted files.
        sourceFileIdentifiers.forEach(x -> {
            File sourceFile = new File(x.getFilePath());
            TsFileResource resource = new TsFileResource(sourceFile);
            if(!sourceFile.exists()){
                // source file has been deleted
                resource.forceMarkDeleted();
            }

            if(targetVersionSet.contains(resource.getVersion())){
                partialDeletedFiles.add(resource);
            }else{
                allDeletedFiles.add(resource);
            }
        });

        // recover target files. Notes that only partial_delete files have target files.
        targetFileIdentifiers.forEach(x ->{
            File tmpTargetFile = new File(x.getFilePath());
            File targetFile = new File(x.getFilePath().replace(IoTDBConstant.INNER_COMPACTION_TMP_FILE_SUFFIX, TsFileConstant.TSFILE_SUFFIX));
            TsFileResource resource;
            if(tmpTargetFile.exists()){
                resource = new TsFileResource(tmpTargetFile);
            }else if(targetFile.exists()){
                resource = new TsFileResource(targetFile);
            }else{
                // target file does not exist, then create empty resource
                resource = new TsFileResource();
                // check if target file is deleted after compaction or not
                x.setFilename(x.getFilename()
                        .replace( IoTDBConstant.INNER_COMPACTION_TMP_FILE_SUFFIX,
                                TsFileConstant.TSFILE_SUFFIX));
                if(deletedTargetFileIdentifiers.contains(x)){
                    // target file is deleted after compaction
                    resource.forceMarkDeleted();
                }
            }
            targetFiles.add(resource);
        });
    }

    @Override
    public boolean equalsOtherTask(AbstractCompactionTask otherTask) {
        if (!(otherTask instanceof SettleCompactionTask)) {
            return false;
        }
        SettleCompactionTask otherSettleCompactionTask = (SettleCompactionTask) otherTask;
        return this.allDeletedFiles.equals(otherSettleCompactionTask.allDeletedFiles)
                && this.partialDeletedFiles.equals(otherSettleCompactionTask.partialDeletedFiles)
                && this.performer.getClass().isInstance(otherSettleCompactionTask.performer);
    }

    @Override
    public long getEstimatedMemoryCost() {
        if(partialDeletedFiles.isEmpty()){
            return 0;
        }else{
            partialDeletedFiles.forEach(x -> {
                try {
                    memoryCost = Math.max(memoryCost,spaceEstimator.estimateInnerCompactionMemory(Collections.singletonList(x)));
                } catch (IOException e) {
                    spaceEstimator.cleanup();
                    LOGGER.error("Meet error when estimate settle compaction memory", e);
                    memoryCost = -1;
                }
            });
            return memoryCost;
        }
    }

    @Override
    public int getProcessedFileNum() {
        return allDeletedFiles.size() + partialDeletedFiles.size();
    }

    @Override
    protected void createSummary() {
        if (performer instanceof FastCompactionPerformer) {
            this.summary = new FastCompactionTaskSummary();
        } else {
            this.summary = new CompactionTaskSummary();
        }
    }

    @Override
    public CompactionTaskType getCompactionTaskType() {
        return CompactionTaskType.SETTLE;
    }
}
