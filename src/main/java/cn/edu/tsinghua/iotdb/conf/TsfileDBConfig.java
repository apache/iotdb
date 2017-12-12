package cn.edu.tsinghua.iotdb.conf;

import java.io.File;

import org.joda.time.DateTimeZone;

public class TsfileDBConfig {

	public static final String CONFIG_NAME = "iotdb-engine.properties";
	
	/**
	 * Port which JDBC server listens to
	 */
	public int rpcPort = 6667;

	/**
	 * Is write ahead log enable
	 */
	public boolean enableWal = true;

	/**
	 * When the total number of write ahead log in the file and memory reaches
	 * the specified size, all the logs are compressed and the unused logs are
	 * removed Increase this value, it will lead to short write pause. Decrease
	 * this value, it will increase IO and CPU consumption
	 */
	public int walCleanupThreshold = 500000;

	/**
	 * When a certain amount of write ahead log is reached, it will be flushed to
	 * disk. It is possible to lose at most flush_wal_threshold operations
	 */
	public int flushWalThreshold = 10000;

	/**
	 * The cycle when write ahead log is periodically refreshed to disk(in
	 * milliseconds) It is possible to lose at most flush_wal_period_in_ms ms
	 * operations
	 */
	public long flushWalPeriodInMs = 10;
	/**
	 * Data directory
	 */
	public String dataDir = null;
	/**
	 * Data directory of Overflow data
	 */
	public String overflowDataDir = "overflow";

	/**
	 * Data directory of fileNode data
	 */
	public String fileNodeDir = "digest";

	/**
	 * Data directory of bufferWrite data
	 */
	public String bufferWriteDir = "delta";

	/**
	 * Data directory of metadata data
	 */
	public String metadataDir = "metadata";

	/**
	 * Data directory of derby data
	 */
	public String derbyHome = "derby";

	/**
	 * Data directory of Write ahead log folder.
	 */
	public String walFolder = "wals";

	/**
	 * The maximum concurrent thread number for merging overflow
	 */
	public int mergeConcurrentThreads = 10;

	/**
	 * Maximum number of folders open at the same time
	 */
	public int maxOpenFolder = 100;

	/**
	 * The amount of data that is read every time when IoTDB merge data.
	 */
	public int fetchSize = 10000;

	/**
	 * the maximum number of writing instances existing in same time.
	 */
	@Deprecated
	public int writeInstanceThreshold = 5;

	/**
	 * The period time of flushing data from memory to file. . The unit is second.
	 */
	public long periodTimeForFlush = 3600;

	/**
	 * The period time for merge overflow data with tsfile data. The unit is
	 * second.
	 */
	public long periodTimeForMerge = 7200;

	public DateTimeZone timeZone = DateTimeZone.getDefault();

	public TsfileDBConfig() {}

	public void updateDataPath() {
		if(dataDir == null){
			dataDir = System.getProperty(TsFileDBConstant.IOTDB_HOME, null);
			if(dataDir == null){
				dataDir = "data";
			} else {
				if (dataDir.length() > 0 && !dataDir.endsWith(File.separator)) {
					dataDir = dataDir + File.separatorChar + "data";
				}
			}
		}
		// filenode dir
		if (dataDir.length() > 0 && !dataDir.endsWith(File.separator)) {
			dataDir = dataDir + File.separatorChar;
		}
		fileNodeDir = dataDir + fileNodeDir;
		bufferWriteDir = dataDir + bufferWriteDir;
		overflowDataDir = dataDir + overflowDataDir;
		metadataDir = dataDir + metadataDir;
		derbyHome = dataDir + derbyHome;
		walFolder = dataDir + walFolder;
	}
}
