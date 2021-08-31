package org.apache.iotdb.db.metadata.mtree.traverser.collector;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.IMNode;

import java.util.List;

public class MNodePathLevelCollector extends CollectorTraverser<List<PartialPath>>{

    public MNodePathLevelCollector(IMNode startNode, String[] nodes, List<PartialPath> resultSet, int targetLevel) {
        super(startNode, nodes, resultSet);
        isLevelTraverser = true;
        this.targetLevel = targetLevel;
    }

    @Override
    protected boolean isValid(IMNode node) {
        return true;
    }

    @Override
    protected void processValidNode(IMNode node, int idx) throws MetadataException {
        resultSet.add(node.getPartialPath());
    }

    @Override
    protected boolean processInternalValid(IMNode node, int idx) throws MetadataException {
        return false;
    }
}
