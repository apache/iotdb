package cn.edu.tsinghua.iotdb.jdbc;

public class TsFileDBConstant {

	public static final String GLOBAL_DB_NAME = "IoTDB";

  public static final String GLOBAL_DB_VERSION = "0.8.0-SNAPSHOT";

	public static final String GLOBAL_COLUMN_REQ = "COLUMN";

	public static final String GLOBAL_DELTA_OBJECT_REQ = "DELTA_OBEJECT";
  
	public static final String GLOBAL_SHOW_TIMESERIES_REQ = "SHOW_TIMESERIES";

	public static final String GLOBAL_SHOW_STORAGE_GROUP_REQ = "SHOW_STORAGE_GROUP";

	public static final String GLOBAL_COLUMNS_REQ = "ALL_COLUMNS";

	// catalog parameters used for DatabaseMetaData.getColumns()
	public static final String CatalogColumn = "col";
	public static final String CatalogTimeseries = "ts";
	public static final String CatalogStorageGroup = "sg";
	public static final String CatalogDeltaObject = "delta";
}
