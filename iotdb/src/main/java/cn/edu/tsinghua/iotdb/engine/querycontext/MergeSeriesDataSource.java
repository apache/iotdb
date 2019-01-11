package cn.edu.tsinghua.iotdb.engine.querycontext;

public class MergeSeriesDataSource {
	
	private OverflowInsertFile insertFile;

	public MergeSeriesDataSource(OverflowInsertFile insertFile) {
		this.insertFile = insertFile;
	}

	public OverflowInsertFile getInsertFile() {
		return insertFile;
	}

}
