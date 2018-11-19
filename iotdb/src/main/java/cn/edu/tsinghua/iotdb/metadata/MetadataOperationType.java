package cn.edu.tsinghua.iotdb.metadata;

public class MetadataOperationType {
	public final static String ADD_PATH_TO_MTREE = "0";
	public final static String DELETE_PATH_FROM_MTREE = "1";
	public final static String SET_STORAGE_LEVEL_TO_MTREE = "2";
	public final static String ADD_A_PTREE = "3";
	public final static String ADD_A_PATH_TO_PTREE = "4";
	public final static String DELETE_PATH_FROM_PTREE = "5";
	public final static String LINK_MNODE_TO_PTREE = "6";
	public final static String UNLINK_MNODE_FROM_PTREE = "7";
	public final static String ADD_INDEX_TO_PATH = "8";
	public final static String DELETE_INDEX_FROM_PATH = "9";
}
