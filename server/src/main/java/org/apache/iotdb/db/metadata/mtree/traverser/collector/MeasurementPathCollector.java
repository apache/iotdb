package org.apache.iotdb.db.metadata.mtree.traverser.collector;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;

import java.util.List;


public class MeasurementPathCollector extends MeasurementCollector<List<PartialPath>>{

    public MeasurementPathCollector(IMNode startNode, String[] nodes, List<PartialPath> resultSet) {
        super(startNode, nodes, resultSet);
    }

    public MeasurementPathCollector(IMNode startNode, String[] nodes, List<PartialPath> resultSet, int limit, int offset) {
        super(startNode, nodes, resultSet, limit, offset);
    }

    @Override
    protected void collectMeasurementSchema(IMeasurementMNode node) throws MetadataException {
        PartialPath path = node.getPartialPath();
        if(nodes[nodes.length - 1].equals(node.getAlias())){
            path.setMeasurementAlias(node.getAlias());
        }
        resultSet.add(path);
    }

    @Override
    protected void collectVectorMeasurementSchema(IMeasurementMNode node, int index) throws MetadataException {
        resultSet.add(node.getPartialPath().concatNode(node.getSchema().getValueMeasurementIdList().get(index)));
    }
}
