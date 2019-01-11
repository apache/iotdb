package cn.edu.tsinghua.iotdb.engine.querycontext;


public class QueryDataSource {

	// sequence data source
	private GlobalSortedSeriesDataSource seriesDataSource;

	// unSequence data source
	private OverflowSeriesDataSource overflowSeriesDataSource;

	public QueryDataSource(GlobalSortedSeriesDataSource seriesDataSource,
			OverflowSeriesDataSource overflowSeriesDataSource) {
		this.seriesDataSource = seriesDataSource;
		this.overflowSeriesDataSource = overflowSeriesDataSource;
	}

	public GlobalSortedSeriesDataSource getSeqDataSource() {
		return seriesDataSource;
	}

	public OverflowSeriesDataSource getOverflowSeriesDataSource() {
		return overflowSeriesDataSource;
	}
}
