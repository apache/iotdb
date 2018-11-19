package cn.edu.tsinghua.tsfile.timeseries.readV2.query.impl;

import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.readV2.common.EncodedSeriesChunkDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.readV2.controller.MetadataQuerier;
import cn.edu.tsinghua.tsfile.timeseries.readV2.controller.SeriesChunkLoader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryExecutor;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryExpression;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.timegenerator.TimestampGenerator;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.timegenerator.TimestampGeneratorByQueryFilterImpl;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.impl.SeriesReaderFromSingleFileByTimestampImpl;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;


/**
 * @author Jinrui Zhang
 */
public class QueryWithQueryFilterExecutorImpl implements QueryExecutor {

    private SeriesChunkLoader seriesChunkLoader;
    private MetadataQuerier metadataQuerier;

    public QueryWithQueryFilterExecutorImpl(SeriesChunkLoader seriesChunkLoader, MetadataQuerier metadataQuerier) {
        this.seriesChunkLoader = seriesChunkLoader;
        this.metadataQuerier = metadataQuerier;
    }

    @Override
    public QueryDataSet execute(QueryExpression queryExpression) throws IOException {
        TimestampGenerator timestampGenerator = new TimestampGeneratorByQueryFilterImpl(queryExpression.getQueryFilter(),
                seriesChunkLoader, metadataQuerier);
        LinkedHashMap<Path, SeriesReaderFromSingleFileByTimestampImpl> readersOfSelectedSeries = new LinkedHashMap<>();
        initReadersOfSelectedSeries(readersOfSelectedSeries, queryExpression.getSelectedSeries());
        return new QueryDataSetForQueryWithQueryFilterImpl(timestampGenerator, readersOfSelectedSeries);
    }

    private void initReadersOfSelectedSeries(LinkedHashMap<Path, SeriesReaderFromSingleFileByTimestampImpl> readersOfSelectedSeries,
                                             List<Path> selectedSeries) throws IOException {
        for (Path path : selectedSeries) {
            List<EncodedSeriesChunkDescriptor> encodedSeriesChunkDescriptorList = metadataQuerier.getSeriesChunkDescriptorList(path);
            SeriesReaderFromSingleFileByTimestampImpl seriesReader = new SeriesReaderFromSingleFileByTimestampImpl(
                    seriesChunkLoader, encodedSeriesChunkDescriptorList);
            readersOfSelectedSeries.put(path, seriesReader);
        }
    }
}