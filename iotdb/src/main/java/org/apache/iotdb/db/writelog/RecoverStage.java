package org.apache.iotdb.db.writelog;

public enum RecoverStage {
    /**
     * This is just the start point of the recovery automata
     */
    init,

    /**
     * In this stage, the mission is to backup restore file, processor.store file with suffix "-recovery".
     * Should SET flag afterward.
     */
    backup,

    /**
     * In this stage, the mission is to recover TsFile / OverflowFile with restore file
     * Should NOT SET flag afterward.
     */
    recoverFile,

    /**
     * In this stage, the mission is to read logs from wal and wal-old files (if exists) and replay them.
     * Should SET flag afterward,
     */
    replayLog,

    /**
     * In this stage, the mission is to clean all "-recovery" files, log file and recovery flag.
     * Should CLEAN flag afterward.
     */
    cleanup
}
