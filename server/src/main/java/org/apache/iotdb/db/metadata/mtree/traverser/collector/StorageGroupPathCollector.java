package org.apache.iotdb.db.metadata.mtree.traverser.collector;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.IMNode;

import java.util.List;

public class StorageGroupPathCollector extends StorageGroupCollector<List<PartialPath>>{

    public StorageGroupPathCollector(IMNode startNode, String[] nodes, List<PartialPath> resultSet) {
        super(startNode, nodes, resultSet);
    }

    public StorageGroupPathCollector(IMNode startNode, String[] nodes, List<PartialPath> resultSet, int limit, int offset) {
        super(startNode, nodes, resultSet, limit, offset);
    }

    @Override
    protected void processValidNode(IMNode node, int idx) throws MetadataException {
        resultSet.add(node.getPartialPath());
    }
}
