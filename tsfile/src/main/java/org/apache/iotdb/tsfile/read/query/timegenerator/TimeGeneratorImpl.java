package org.apache.iotdb.tsfile.read.query.timegenerator;

import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.read.expression.ExpressionType;
import org.apache.iotdb.tsfile.read.expression.IBinaryExpression;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.controller.MetadataQuerier;
import org.apache.iotdb.tsfile.read.controller.ChunkLoader;
import org.apache.iotdb.tsfile.read.query.timegenerator.node.AndNode;
import org.apache.iotdb.tsfile.read.query.timegenerator.node.LeafNode;
import org.apache.iotdb.tsfile.read.query.timegenerator.node.Node;
import org.apache.iotdb.tsfile.read.query.timegenerator.node.OrNode;
import org.apache.iotdb.tsfile.read.reader.series.FileSeriesReader;
import org.apache.iotdb.tsfile.read.reader.series.FileSeriesReaderWithFilter;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.controller.ChunkLoader;
import org.apache.iotdb.tsfile.read.controller.MetadataQuerier;
import org.apache.iotdb.tsfile.read.expression.ExpressionType;
import org.apache.iotdb.tsfile.read.expression.IBinaryExpression;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.query.timegenerator.node.AndNode;
import org.apache.iotdb.tsfile.read.query.timegenerator.node.Node;
import org.apache.iotdb.tsfile.read.query.timegenerator.node.OrNode;
import org.apache.iotdb.tsfile.read.reader.series.FileSeriesReader;
import org.apache.iotdb.tsfile.read.reader.series.FileSeriesReaderWithFilter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


public class TimeGeneratorImpl implements TimeGenerator {

    private ChunkLoader chunkLoader;
    private MetadataQuerier metadataQuerier;
    private Node operatorNode;

    private HashMap<Path, List<LeafNode>> leafCache;

    public TimeGeneratorImpl(IExpression IExpression, ChunkLoader chunkLoader
            , MetadataQuerier metadataQuerier) throws IOException {
        this.chunkLoader = chunkLoader;
        this.metadataQuerier = metadataQuerier;
        this.leafCache = new HashMap<>();

        operatorNode = construct(IExpression);
    }

    @Override
    public boolean hasNext() throws IOException {
        return operatorNode.hasNext();
    }

    @Override
    public long next() throws IOException {
        return operatorNode.next();
    }

    @Override
    public Object getValue(Path path, long time) {

        for (LeafNode leafNode : leafCache.get(path)) {
            if(!leafNode.currentTimeIs(time))
                continue;
            return leafNode.currentValue(time);
        }

        return null;
    }


    /**
     * construct the tree that generate timestamp
     */
    private Node construct(IExpression expression) throws IOException {

        if (expression.getType() == ExpressionType.SERIES) {
            SingleSeriesExpression singleSeriesExp = (SingleSeriesExpression) expression;
            FileSeriesReader seriesReader = generateSeriesReader(singleSeriesExp);
            Path path = singleSeriesExp.getSeriesPath();

            if (!leafCache.containsKey(path))
                leafCache.put(path, new ArrayList<>());

            // put the current reader to valueCache
            LeafNode leafNode = new LeafNode(seriesReader);
            leafCache.get(path).add(leafNode);

            return leafNode;

        } else if (expression.getType() == ExpressionType.OR) {
            Node leftChild = construct(((IBinaryExpression) expression).getLeft());
            Node rightChild = construct(((IBinaryExpression) expression).getRight());
            return new OrNode(leftChild, rightChild);

        } else if (expression.getType() == ExpressionType.AND) {
            Node leftChild = construct(((IBinaryExpression) expression).getLeft());
            Node rightChild = construct(((IBinaryExpression) expression).getRight());
            return new AndNode(leftChild, rightChild);
        }
        throw new UnSupportedDataTypeException("Unsupported ExpressionType when construct OperatorNode: " + expression.getType());
    }

    private FileSeriesReader generateSeriesReader(SingleSeriesExpression singleSeriesExp) throws IOException {
        List<ChunkMetaData> chunkMetaDataList = metadataQuerier.getChunkMetaDataList(
                singleSeriesExp.getSeriesPath());
        return new FileSeriesReaderWithFilter(chunkLoader, chunkMetaDataList, singleSeriesExp.getFilter());
    }
}
