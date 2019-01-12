package org.apache.iotdb.db.concurrent;

public enum ThreadName {
    JDBC_SERVICE("JDBC-ServerServiceImpl"),
    JDBC_CLIENT("JDBC-Client"),
    MERGE_SERVICE("Merge-ServerServiceImpl"),
    CLOSE_MERGE_SERVICE("Close-Merge-ServerServiceImpl"),
    CLOSE_MERGE_DAEMON("Close-Merge-Daemon-Thread"),
    CLOSE_DAEMON("Close-Daemon-Thread"),
    MERGE_DAEMON("Merge-Daemon-Thread"),
    MEMORY_MONITOR("IoTDB-MemMonitor-Thread"),
    MEMORY_STATISTICS("IoTDB-MemStatistic-Thread"),
    FLUSH_PARTIAL_POLICY("IoTDB-FlushPartialPolicy-Thread"),
    FORCE_FLUSH_ALL_POLICY("IoTDB-ForceFlushAllPolicy-Thread"),
    STAT_MONITOR("StatMonitor-ServerServiceImpl"),
    FLUSH_SERVICE("Flush-ServerServiceImpl"),
    WAL_DAEMON("IoTDB-MultiFileLogNodeManager-Sync-Thread"),
    INDEX_SERVICE("Index-ServerServiceImpl");
    
    private String name;
    
    private ThreadName(String name){
    	this.name = name;
    }
    
    public String getName(){
    	return name;
    }
}
