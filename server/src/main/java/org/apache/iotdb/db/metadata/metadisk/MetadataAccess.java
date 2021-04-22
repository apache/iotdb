package org.apache.iotdb.db.metadata.metadisk;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.mnode.MNode;

import java.util.Map;

public interface MetadataAccess {

    MNode getRoot() throws MetadataException;

    MNode getChild(MNode parent, String name) throws MetadataException;

    Map<String, MNode> getChildren(MNode parent) throws MetadataException;

    void addChild(MNode parent, String childName, MNode child) throws MetadataException;

    void addAlias(MNode parent, String alias, MNode child) throws MetadataException;

    void replaceChild(MNode parent, String measurement, MNode newChild)
            throws MetadataException;

    void deleteChild(MNode parent, String childName) throws MetadataException;

    void deleteAliasChild(MNode parent, String alias) throws MetadataException;


}
