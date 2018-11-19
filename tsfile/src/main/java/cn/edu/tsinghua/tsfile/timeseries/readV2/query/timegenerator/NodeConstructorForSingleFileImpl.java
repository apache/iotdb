package cn.edu.tsinghua.tsfile.timeseries.readV2.query.timegenerator;

import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.impl.SeriesFilter;
import cn.edu.tsinghua.tsfile.timeseries.readV2.common.EncodedSeriesChunkDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.readV2.controller.MetadataQuerier;
import cn.edu.tsinghua.tsfile.timeseries.readV2.controller.SeriesChunkLoader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.SeriesReader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.impl.SeriesReaderFromSingleFileWithFilterImpl;

import java.io.IOException;
import java.util.List;

/**
 * Created by zhangjinrui on 2017/12/26.
 */
public class NodeConstructorForSingleFileImpl extends NodeConstructor {
    private MetadataQuerier metadataQuerier;
    private SeriesChunkLoader seriesChunkLoader;

    public NodeConstructorForSingleFileImpl(MetadataQuerier metadataQuerier, SeriesChunkLoader seriesChunkLoader) {
        this.metadataQuerier = metadataQuerier;
        this.seriesChunkLoader = seriesChunkLoader;
    }

    @Override
    public SeriesReader generateSeriesReader(SeriesFilter<?> seriesFilter) throws IOException {
        List<EncodedSeriesChunkDescriptor> encodedSeriesChunkDescriptorList = metadataQuerier.getSeriesChunkDescriptorList(
                seriesFilter.getSeriesPath());
        return new SeriesReaderFromSingleFileWithFilterImpl(seriesChunkLoader, encodedSeriesChunkDescriptorList, seriesFilter.getFilter());
    }
}
