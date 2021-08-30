package org.apache.iotdb.db.metadata.mtree.traverser.collector;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.mnode.IMNode;

import java.util.Map;


public class StorageGroupDeterminator extends StorageGroupCollector<Map<String, String>>{


    public StorageGroupDeterminator(IMNode startNode, String[] nodes, Map<String, String> resultSet) {
        super(startNode, nodes, resultSet);
        collectInternal = true;
    }

    @Override
    protected void processValidNode(IMNode node, int idx) throws MetadataException {
        // we have found one storage group, record it
        String sgName = node.getFullPath();
        // concat the remaining path with the storage group name
        StringBuilder pathWithKnownSG = new StringBuilder(sgName);
        for (int i = idx + 1; i < nodes.length; i++) {
            pathWithKnownSG.append(IoTDBConstant.PATH_SEPARATOR).append(nodes[i]);
        }
        if (idx >= nodes.length && nodes[nodes.length-1].equals(PATH_MULTI_LEVEL_WILDCARD)) {
            // the we find the sg match the last node and the last node is a wildcard (find "root
            // .group1", for "root.**"), also append the wildcard (to make "root.group1.**")
            pathWithKnownSG.append(IoTDBConstant.PATH_SEPARATOR).append(PATH_MULTI_LEVEL_WILDCARD);
        }
        resultSet.put(sgName, pathWithKnownSG.toString());
    }
}
