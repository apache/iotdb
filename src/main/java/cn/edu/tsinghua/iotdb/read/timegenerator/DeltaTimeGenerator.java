package cn.edu.tsinghua.iotdb.read.timegenerator;

import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.QueryFilter;
import cn.edu.tsinghua.tsfile.timeseries.readV2.controller.MetadataQuerier;
import cn.edu.tsinghua.tsfile.timeseries.readV2.controller.SeriesChunkLoader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.timegenerator.NodeConstructorForSingleFileImpl;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.timegenerator.TimestampGenerator;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.timegenerator.node.Node;

import java.io.IOException;

public class DeltaTimeGenerator implements TimestampGenerator {

    private QueryFilter queryFilter;
    private Node operatorNode;

    public DeltaTimeGenerator(QueryFilter queryFilter) throws IOException {
        this.queryFilter = queryFilter;
    }

    private void initNode(SeriesChunkLoader seriesChunkLoader, MetadataQuerier metadataQuerier) throws IOException {
        NodeConstructorForSingleFileImpl nodeConstructorForSingleFile = new NodeConstructorForSingleFileImpl(metadataQuerier, seriesChunkLoader);
        this.operatorNode = nodeConstructorForSingleFile.construct(queryFilter);
    }

    @Override
    public boolean hasNext() throws IOException {
        return false;
    }

    @Override
    public long next() throws IOException {
        return 0;
    }
}
