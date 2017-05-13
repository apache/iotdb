package cn.edu.thu.tsfiledb.conf;

public class TSFileDBConfig {
	/**
	 * the maximum number of writing instances existing in same time.
	 */
	public int writeInstanceThreshold = 5;

	/**
	 * data directory of Overflow data
	 */
	public String overflowDataDir = "src/main/resources/data/overflow";
	/**
	 * data directory of fileNode data
	 */
	public String FileNodeDir = "src/main/resources/data/digest";
	/**
	 * data directory of bufferWrite data
	 */
	public String BufferWriteDir = "src/main/resources/data/delta";

	public String metadataDir = "src/main/resources/metadata";

	public String derbyHome = "src/main/resources/derby";

	/**
	 * maximum concurrent thread number for merging overflow
	 */
	public int mergeConcurrentThreadNum = 10;
	/**
	 * the maximum number of concurrent file node instances
	 */
	public int maxFileNodeNum = 1000;
	/**
	 * the maximum number of concurrent overflow instances
	 */
	public int maxOverflowNodeNum = 100;
	/**
	 * the maximum number of concurrent buffer write instances
	 */
	public int maxBufferWriteNodeNum = 50;
	public int defaultFetchSize = 1000000;
	public String writeLogPath = "src/main/resources/writeLog.log";

	public TSFileDBConfig() {
	}

}
