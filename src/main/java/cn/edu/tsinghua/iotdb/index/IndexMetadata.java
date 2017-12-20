package cn.edu.tsinghua.iotdb.index;

public class IndexMetadata {
	public String timeseries;
	public boolean isIndexExisted;
	
	public IndexMetadata(String timeseries, boolean isIndexExisted){
		this.timeseries = timeseries;
		this.isIndexExisted = isIndexExisted;
	}
}
