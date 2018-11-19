package cn.edu.tsinghua.tsfile.timeseries.readV2.query.impl;

import cn.edu.tsinghua.tsfile.timeseries.filterV2.basic.Filter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.impl.GlobalTimeFilter;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.readV2.common.EncodedSeriesChunkDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.readV2.controller.MetadataQuerier;
import cn.edu.tsinghua.tsfile.timeseries.readV2.controller.SeriesChunkLoader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryExecutor;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryExpression;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.SeriesReader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.impl.SeriesReaderFromSingleFileWithFilterImpl;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * Created by zhangjinrui on 2017/12/27.
 */
public class QueryWithGlobalTimeFilterExecutorImpl implements QueryExecutor {

    private SeriesChunkLoader seriesChunkLoader;
    private MetadataQuerier metadataQuerier;

    public QueryWithGlobalTimeFilterExecutorImpl(SeriesChunkLoader seriesChunkLoader, MetadataQuerier metadataQuerier) {
        this.seriesChunkLoader = seriesChunkLoader;
        this.metadataQuerier = metadataQuerier;
    }

    @Override
    public QueryDataSet execute(QueryExpression queryExpression) throws IOException {
        LinkedHashMap<Path, SeriesReader> readersOfSelectedSeries = new LinkedHashMap<>();
        Filter<Long> timeFilter = ((GlobalTimeFilter) queryExpression.getQueryFilter()).getFilter();
        initReadersOfSelectedSeries(readersOfSelectedSeries, queryExpression.getSelectedSeries(), timeFilter);
        return new MergeQueryDataSet(readersOfSelectedSeries);
    }

    private void initReadersOfSelectedSeries(LinkedHashMap<Path, SeriesReader> readersOfSelectedSeries,
                                             List<Path> selectedSeries, Filter<Long> timeFilter) throws IOException {
        for (Path path : selectedSeries) {
            List<EncodedSeriesChunkDescriptor> encodedSeriesChunkDescriptorList = metadataQuerier.getSeriesChunkDescriptorList(path);
            SeriesReader seriesReader = new SeriesReaderFromSingleFileWithFilterImpl(seriesChunkLoader, encodedSeriesChunkDescriptorList, timeFilter);
            readersOfSelectedSeries.put(path, seriesReader);
        }
    }
}
