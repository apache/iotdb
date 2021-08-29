package org.apache.iotdb.db.metadata.mtree.traverser.collector;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.IMNode;

import java.util.Set;

public class EntityPathCollector extends EntityCollector<Set<PartialPath>>{

    public EntityPathCollector(IMNode startNode, String[] nodes, Set<PartialPath> resultSet) {
        super(startNode, nodes, resultSet);
    }

    public EntityPathCollector(IMNode startNode, String[] nodes, Set<PartialPath> resultSet, int limit, int offset) {
        super(startNode, nodes, resultSet, limit, offset);
    }

    @Override
    protected void processValidNode(IMNode node, int idx) throws MetadataException {
        resultSet.add(node.getPartialPath());
    }
}
