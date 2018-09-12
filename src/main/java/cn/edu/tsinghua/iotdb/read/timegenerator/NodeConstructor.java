package cn.edu.tsinghua.iotdb.read.timegenerator;

import cn.edu.tsinghua.iotdb.engine.querycontext.QueryDataSource;
import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;
import cn.edu.tsinghua.iotdb.read.QueryDataSourceExecutor;
import cn.edu.tsinghua.iotdb.read.reader.QueryWithOrWithOutFilterReader;
import cn.edu.tsinghua.tsfile.common.exception.UnSupportedDataTypeException;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.BinaryQueryFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.QueryFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.QueryFilterType;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.impl.SeriesFilter;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.timegenerator.node.AndNode;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.timegenerator.node.LeafNode;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.timegenerator.node.Node;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.timegenerator.node.OrNode;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.SeriesReader;


import java.io.IOException;

public class NodeConstructor {

    public NodeConstructor() {
    }

    public Node construct(QueryFilter queryFilter) throws IOException, FileNodeManagerException {
        if (queryFilter.getType() == QueryFilterType.SERIES) {
            return new LeafNode(this.generateSeriesReader((SeriesFilter)queryFilter));
        } else {
            Node leftChild;
            Node rightChild;
            if (queryFilter.getType() == QueryFilterType.OR) {
                leftChild = this.construct(((BinaryQueryFilter)queryFilter).getLeft());
                rightChild = this.construct(((BinaryQueryFilter)queryFilter).getRight());
                return new OrNode(leftChild, rightChild);
            } else if (queryFilter.getType() == QueryFilterType.AND) {
                leftChild = this.construct(((BinaryQueryFilter)queryFilter).getLeft());
                rightChild = this.construct(((BinaryQueryFilter)queryFilter).getRight());
                return new AndNode(leftChild, rightChild);
            } else {
                throw new UnSupportedDataTypeException("Unsupported QueryFilterType when construct OperatorNode: " + queryFilter.getType());
            }
        }
    }

    public SeriesReader generateSeriesReader(SeriesFilter<?> seriesFilter) throws IOException, FileNodeManagerException {
        QueryDataSource queryDataSource = QueryDataSourceExecutor.getQueryDataSource(seriesFilter);
        return new QueryWithOrWithOutFilterReader(queryDataSource, seriesFilter);
    }


}
