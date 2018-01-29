package cn.edu.tsinghua.iotdb.concurrent;

public enum ThreadName {
    JDBC_SERVICE("JDBC-Service"),
    JDBC_CLIENT("JDBC-Client"),
    MERGE_SERVICE("Merge-Service"),
    CLOSE_MERGE_SERVICE("Close-Merge-Service"),
    CLOSE_MERGE_DAEMON("Close-Merge-Daemon-Thread"),
    CLOSE_DAEMON("Close-Daemon-Thread"),
    MERGE_DAEMON("Merge-Daemon-Thread"),
    MEMORY_MONITOR("IoTDB-MemMonitor-Thread"),
    MEMORY_STATISTICS("IoTDB-MemStatistic-Thread"),
    FLUSH_PARTIAL_POLICY("IoTDB-FlushPartialPolicy-Thread"),
    FORCE_FLUSH_ALL_POLICY("IoTDB-ForceFlushAllPolicy-Thread"),
    STAT_MONITOR("StatMonitor-Service"),
    FLUSH_SERVICE("Flush-Service"),
    WAL_DAEMON("IoTDB-MultiFileLogNodeManager-Sync-Thread"),
    INDEX_SERVICE("Index-Service");
    
    private String name;
    
    private ThreadName(String name){
    	this.name = name;
    }
    
    public String getName(){
    	return name;
    }
}
