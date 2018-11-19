package cn.edu.tsinghua.iotdb.engine.querycontext;

public class MergeSeriesDataSource {
	
	private OverflowInsertFile insertFile;
	private OverflowUpdateDeleteFile updateDeleteFile;
	
	public MergeSeriesDataSource(OverflowInsertFile insertFile, OverflowUpdateDeleteFile updateDeleteFile) {
		super();
		this.insertFile = insertFile;
		this.updateDeleteFile = updateDeleteFile;
	}

	public OverflowInsertFile getInsertFile() {
		return insertFile;
	}

	public OverflowUpdateDeleteFile getUpdateDeleteFile() {
		return updateDeleteFile;
	}
	
}
