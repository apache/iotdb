package cn.edu.tsinghua.iotdb.engine.bufferwrite;

/**
 * Constants for using in bufferwrite, overflow and filenode
 * 
 * @author liukun
 *
 */
public class FileNodeConstants {

	public static final String FILE_NODE_OPERATOR_TYPE = "OPERATOR_TYPE";
	public static final String TIMESTAMP_KEY = "TIMESTAMP";
	public static final String FILE_NODE = "FILE_NODE";
	public static final String CLOSE_ACTION = "CLOSE_ACTION";

	public static final String OVERFLOW_FLUSH_ACTION = "OVERFLOW_FLUSH_ACTION";
	public static final String BUFFERWRITE_FLUSH_ACTION = "BUFFERWRITE_FLUSH_ACTION";
	public static final String BUFFERWRITE_CLOSE_ACTION = "BUFFERWRITE_CLOSE_ACTION";
	public static final String FILENODE_PROCESSOR_FLUSH_ACTION = "FILENODE_PROCESSOR_FLUSH_ACTION";
	
	public static final String MREGE_EXTENSION = "merge";
	public static final String ERR_EXTENSION = "err";
	public static final String PATH_SEPARATOR = ".";
	public static final String BUFFERWRITE_FILE_SEPARATOR= "-";

}
