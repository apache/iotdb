package cn.edu.tsinghua.iotdb.engine.querycontext;

/**
 * @author liukun
 */
public class QueryDataSource {

	private GlobalSortedSeriesDataSource seriesDataSource;
	private OverflowSeriesDataSource overflowSeriesDataSource;

	public QueryDataSource(GlobalSortedSeriesDataSource seriesDataSource,
			OverflowSeriesDataSource overflowSeriesDataSource) {
		this.seriesDataSource = seriesDataSource;
		this.overflowSeriesDataSource = overflowSeriesDataSource;
	}

	public GlobalSortedSeriesDataSource getSeriesDataSource() {
		return seriesDataSource;
	}

	public OverflowSeriesDataSource getOverflowSeriesDataSource() {
		return overflowSeriesDataSource;
	}
}
