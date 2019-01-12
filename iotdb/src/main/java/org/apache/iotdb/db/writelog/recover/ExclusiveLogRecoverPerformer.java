package org.apache.iotdb.db.writelog.recover;

import org.apache.iotdb.db.engine.filenode.FileNodeManager;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.exception.RecoverException;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.writelog.RecoverStage;
import org.apache.iotdb.db.writelog.replay.ConcreteLogReplayer;
import org.apache.iotdb.db.writelog.io.RAFLogReader;
import org.apache.iotdb.db.writelog.replay.LogReplayer;
import org.apache.iotdb.db.writelog.node.ExclusiveWriteLogNode;
import org.apache.commons.io.FileUtils;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.exception.RecoverException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.writelog.RecoverStage.*;

public class ExclusiveLogRecoverPerformer implements RecoverPerformer {

    private static final Logger logger = LoggerFactory.getLogger(ExclusiveLogRecoverPerformer.class);

    public static final String RECOVER_FLAG_NAME = "recover-flag";

    public static final String RECOVER_SUFFIX = "-recover";

    public static final String FLAG_SEPERATOR = "-";

    private ExclusiveWriteLogNode writeLogNode;

    private String recoveryFlagPath;

    private String restoreFilePath;

    private String processorStoreFilePath;

    private RecoverStage currStage;

    private LogReplayer replayer = new ConcreteLogReplayer();

    // The two fields can be made static only because the recovery is a serial process.
    private static RAFLogReader RAFLogReader = new RAFLogReader();

    private RecoverPerformer fileNodeRecoverPerformer;

    public ExclusiveLogRecoverPerformer(String restoreFilePath, String processorStoreFilePath, ExclusiveWriteLogNode logNode) {
        this.restoreFilePath = restoreFilePath;
        this.processorStoreFilePath = processorStoreFilePath;
        this.writeLogNode = logNode;
        this.fileNodeRecoverPerformer = new FileNodeRecoverPerformer(writeLogNode.getIdentifier());
    }

    public void setFileNodeRecoverPerformer(RecoverPerformer fileNodeRecoverPerformer) {
        this.fileNodeRecoverPerformer = fileNodeRecoverPerformer;
    }

    public void setReplayer(LogReplayer replayer) {
        this.replayer = replayer;
    }

    @Override
    public void recover() throws RecoverException {
        currStage = determineStage();
       if(currStage != null)
           recoverAtStage(currStage);
    }

    private RecoverStage determineStage() throws RecoverException {
        File logDir = new File(writeLogNode.getLogDirectory());
        if(!logDir.exists()) {
            logger.error("Log node {} directory does not exist, recover failed", writeLogNode.getLogDirectory());
            throw new RecoverException("No directory for log node " + writeLogNode.getIdentifier());
        }
        // search for the flag file
        File[] files = logDir.listFiles((dir, name) -> name.contains(RECOVER_FLAG_NAME));

        if(files == null || files.length == 0) {
            File[] logFiles = logDir.listFiles((dir, name) -> name.contains(ExclusiveWriteLogNode.WAL_FILE_NAME));
            // no flag is set, and there exists log file, start from beginning.
            if(logFiles != null && logFiles.length > 0)
                return RecoverStage.backup;
                // no flag is set, and there is no log file, do not recover.
            else
                return null;
        }

        File flagFile = files[0];
        String flagName = flagFile.getName();
        recoveryFlagPath = flagFile.getPath();
        // the flag name is like "recover-flag-{flagType}"
        String[] parts = flagName.split(FLAG_SEPERATOR);
        if(parts.length != 3) {
            logger.error("Log node {} invalid recover flag name {}", writeLogNode.getIdentifier(), flagName);
            throw new RecoverException("Illegal recover flag " + flagName);
        }
        String stageName = parts[2];
        // if a flag of stage X is found, that means X had finished, so start from next stage
        if(stageName.equals(backup.name()))
            return recoverFile;
        else if(stageName.equals(replayLog.name()))
            return cleanup;
        else {
            logger.error("Log node {} invalid recover flag name {}", writeLogNode.getIdentifier(), flagName);
            throw new RecoverException("Illegal recover flag " + flagName);
        }
    }

    private void recoverAtStage(RecoverStage stage) throws RecoverException {
        switch (stage) {
            case init:
            case backup:
                backup();
                break;
            case recoverFile:
                recoverFile();
                break;
            case replayLog:
                replayLog();
                break;
            case cleanup:
                cleanup();
                break;
            default:
                logger.error("Invalid stage {}", stage);
        }
    }

    private void setFlag(RecoverStage stage) {
        if(recoveryFlagPath == null) {
            recoveryFlagPath = writeLogNode.getLogDirectory() + File.separator + RECOVER_FLAG_NAME + FLAG_SEPERATOR + stage.name();
            try {
                File flagFile = new File(recoveryFlagPath);
                if(!flagFile.createNewFile())
                    logger.error("Log node {} cannot set flag at stage {}",writeLogNode.getLogDirectory(), stage.name());
            } catch (IOException e) {
                logger.error("Log node {} cannot set flag at stage {}",writeLogNode.getLogDirectory(), stage.name());
            }
        } else {
            File flagFile = new File(recoveryFlagPath);
            recoveryFlagPath = recoveryFlagPath.replace(FLAG_SEPERATOR + currStage.name(), FLAG_SEPERATOR + stage.name());
            if(!flagFile.renameTo(new File(recoveryFlagPath)))
                logger.error("Log node {} cannot update flag at stage {}",writeLogNode.getLogDirectory(), stage.name());
        }
    }

    private void cleanFlag() throws RecoverException {
        if(recoveryFlagPath != null) {
            File flagFile = new File(recoveryFlagPath);
            if(!flagFile.delete()) {
                logger.error("Log node {} cannot clean flag ", writeLogNode.getLogDirectory());
                throw new RecoverException("Cannot clean flag");
            }
        }
    }

    private void backup() throws RecoverException {
        String recoverRestoreFilePath = restoreFilePath + RECOVER_SUFFIX;
        File recoverRestoreFile = new File(recoverRestoreFilePath);
        File restoreFile = new File(restoreFilePath);
        if(!recoverRestoreFile.exists() && restoreFile.exists()) {
            try {
                FileUtils.copyFile(restoreFile, recoverRestoreFile);
            } catch (Exception e) {
                logger.error("Log node {} cannot backup restore file, because {}", writeLogNode.getLogDirectory(), e.getMessage());
                throw new RecoverException("Cannot backup restore file, recovery aborted.");
            }
        }

        String recoverProcessorStoreFilePath = processorStoreFilePath + RECOVER_SUFFIX;
        File recoverProcessorStoreFile = new File(recoverProcessorStoreFilePath);
        File processorStoreFile = new File(processorStoreFilePath);
        if(!recoverProcessorStoreFile.exists() && processorStoreFile.exists()) {
            try {
                FileUtils.copyFile(processorStoreFile, recoverProcessorStoreFile);
            } catch (Exception e) {
                logger.error("Log node {} cannot backup processor file, because {}", writeLogNode.getLogDirectory(), e.getMessage());
                throw new RecoverException("Cannot backup processor file, recovery aborted.");
            }
        }

        setFlag(backup);
        currStage = recoverFile;
        logger.info("Log node {} backup ended", writeLogNode.getLogDirectory());
        recoverFile();
    }

    private void recoverFile() throws RecoverException {
        String recoverRestoreFilePath = restoreFilePath + RECOVER_SUFFIX;
        File recoverRestoreFile = new File(recoverRestoreFilePath);
        try {
            if(recoverRestoreFile.exists())
                FileUtils.copyFile(recoverRestoreFile, new File(restoreFilePath));
        } catch (Exception e) {
            logger.error("Log node {} cannot recover restore file because {}", writeLogNode.getLogDirectory(), e.getMessage());
            throw new RecoverException("Cannot recover restore file, recovery aborted.");
        }

        String recoverProcessorStoreFilePath = processorStoreFilePath + RECOVER_SUFFIX;
        File recoverProcessorStoreFile = new File(recoverProcessorStoreFilePath);
        try {
            if(recoverProcessorStoreFile.exists())
                FileUtils.copyFile(recoverProcessorStoreFile, new File(processorStoreFilePath) );
        } catch (Exception e) {
            logger.error("Log node {} cannot recover processor file, because{}", writeLogNode.getLogDirectory(), e.getMessage());
            throw new RecoverException("Cannot recover processor file, recovery aborted.");
        }

        fileNodeRecoverPerformer.recover();

        currStage = replayLog;
        logger.info("Log node {} recover files ended", writeLogNode.getLogDirectory());
        replayLog();
    }

    private int replayLogFile(File logFile) throws RecoverException {
        int failedCnt = 0;
        if(logFile.exists()) {
            try {
                RAFLogReader.open(logFile);
            } catch (FileNotFoundException e) {
                logger.error("Log node {} cannot read old log file, because {}",writeLogNode.getIdentifier(), e.getMessage());
                throw new RecoverException("Cannot read old log file, recovery aborted.");
            }
            while(RAFLogReader.hasNext()) {
                try {
                    PhysicalPlan physicalPlan = RAFLogReader.next();
                    if(physicalPlan == null) {
                        logger.error("Log node {} read a bad log",writeLogNode.getIdentifier());
                        throw new RecoverException("Cannot read old log file, recovery aborted.");
                    }
                    replayer.replay(physicalPlan);
                } catch (ProcessorException e) {
                    failedCnt ++;
                    logger.error("Log node {}, {}", writeLogNode.getLogDirectory(), e.getMessage());
                }
            }
            RAFLogReader.close();
        }
        return failedCnt;
    }

    private void replayLog() throws RecoverException {
        int failedEntryCnt = 0;
        // if old log file exists, replay it first.
        File oldLogFile = new File(writeLogNode.getLogDirectory() + File.separator +
                ExclusiveWriteLogNode.WAL_FILE_NAME + ExclusiveWriteLogNode.OLD_SUFFIX);
        failedEntryCnt += replayLogFile(oldLogFile);
        // then replay new log
        File newLogFile = new File(writeLogNode.getLogDirectory() + File.separator + ExclusiveWriteLogNode.WAL_FILE_NAME);
        failedEntryCnt += replayLogFile(newLogFile);
        // TODO : do we need to proceed if there are failed logs ?
        if(failedEntryCnt > 0)
            throw new RecoverException("There are " + failedEntryCnt + " logs failed to recover, see logs above for details");
        try {
            FileNodeManager.getInstance().closeOneFileNode(writeLogNode.getFileNodeName());
        } catch (FileNodeManagerException e) {
            logger.error("Log node {} cannot perform flush after replaying logs! Because {}",writeLogNode.getIdentifier(), e.getMessage());
            throw new RecoverException(e);
        }
        currStage = cleanup;
        setFlag(replayLog);
        logger.info("Log node {} replay ended.", writeLogNode.getLogDirectory());
        cleanup();
    }

    private void cleanup() throws RecoverException {
        // clean recovery files
        List<String> failedFiles = new ArrayList<>();
        String recoverRestoreFilePath = restoreFilePath + RECOVER_SUFFIX;
        File recoverRestoreFile = new File(recoverRestoreFilePath);
        if(recoverRestoreFile.exists()) {
            if(!recoverRestoreFile.delete()){
                logger.error("Log node {} cannot delete backup restore file", writeLogNode.getLogDirectory());
                failedFiles.add(recoverRestoreFilePath);
            }
        }
        String recoverProcessorStoreFilePath = processorStoreFilePath + RECOVER_SUFFIX;
        File recoverProcessorStoreFile = new File(recoverProcessorStoreFilePath);
        if(recoverProcessorStoreFile.exists()) {
            if(!recoverProcessorStoreFile.delete()) {
                logger.error("Log node {} cannot delete backup processor store file", writeLogNode.getLogDirectory());
                failedFiles.add(recoverProcessorStoreFilePath);
            }
        }
        // clean log file
        File oldLogFile = new File(writeLogNode.getLogDirectory() + File.separator +
                ExclusiveWriteLogNode.WAL_FILE_NAME + ExclusiveWriteLogNode.OLD_SUFFIX);
        if(oldLogFile.exists()) {
            if(!oldLogFile.delete()) {
                logger.error("Log node {} cannot delete old log file", writeLogNode.getLogDirectory());
                failedFiles.add(oldLogFile.getPath());
            }
        }
        File newLogFile = new File(writeLogNode.getLogDirectory() + File.separator + ExclusiveWriteLogNode.WAL_FILE_NAME);
        if(newLogFile.exists()) {
            if(!newLogFile.delete()) {
                logger.error("Log node {} cannot delete new log file", writeLogNode.getLogDirectory());
                failedFiles.add(newLogFile.getPath());
            }
        }
        if(failedFiles.size() > 0)
            throw new RecoverException("File clean failed. Failed files are " + failedFiles.toString());
        // clean flag
        currStage = init;
        cleanFlag();
        logger.info("Log node {} cleanup ended.", writeLogNode.getLogDirectory());
    }
}
