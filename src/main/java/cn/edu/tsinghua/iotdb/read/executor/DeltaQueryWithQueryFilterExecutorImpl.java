package cn.edu.tsinghua.iotdb.read.executor;

import cn.edu.tsinghua.iotdb.read.timegenerator.DeltaTimeGenerator;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.readV2.common.EncodedSeriesChunkDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryExecutor;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryExpression;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.impl.QueryDataSetForQueryWithQueryFilterImpl;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.timegenerator.TimestampGenerator;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.timegenerator.TimestampGeneratorByQueryFilterImpl;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.impl.SeriesReaderFromSingleFileByTimestampImpl;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;

public class DeltaQueryWithQueryFilterExecutorImpl implements QueryExecutor {

    public DeltaQueryWithQueryFilterExecutorImpl() {}

    @Override
    public QueryDataSet execute(QueryExpression queryExpression) throws IOException {
        TimestampGenerator timestampGenerator = new DeltaTimeGenerator(queryExpression.getQueryFilter());
        LinkedHashMap<Path, SeriesReaderFromSingleFileByTimestampImpl> readersOfSelectedSeries = new LinkedHashMap<>();
        initReadersOfSelectedSeries(readersOfSelectedSeries, queryExpression.getSelectedSeries());
        return new QueryDataSetForQueryWithQueryFilterImpl(timestampGenerator, readersOfSelectedSeries);
    }

    private void initReadersOfSelectedSeries(LinkedHashMap<Path, SeriesReaderFromSingleFileByTimestampImpl> readersOfSelectedSeries,
                                             List<Path> selectedSeries) throws IOException {
//        for (Path path : selectedSeries) {
//            List<EncodedSeriesChunkDescriptor> encodedSeriesChunkDescriptorList = metadataQuerier.getSeriesChunkDescriptorList(path);
//            SeriesReaderFromSingleFileByTimestampImpl seriesReader = new SeriesReaderFromSingleFileByTimestampImpl(
//                    seriesChunkLoader, encodedSeriesChunkDescriptorList);
//            readersOfSelectedSeries.put(path, seriesReader);
//        }
    }
}
