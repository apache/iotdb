package org.apache.iotdb.db.metadata.mnode;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.logfile.MLogWriter;
import org.apache.iotdb.db.metadata.template.Template;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

public interface IMNode extends Serializable {
    boolean hasChild(String name);

    void addChild(String name, IMNode child);

    IMNode addChild(IMNode child);

    void deleteChild(String name);

    void deleteAliasChild(String alias);

    Template getDeviceTemplate();

    void setDeviceTemplate(Template deviceTemplate);

    IMNode getChild(String name);

    IMNode getChildOfAlignedTimeseries(String name) throws MetadataException;

    int getMeasurementMNodeCount();

    boolean addAlias(String alias, IMNode child);

    String getFullPath();

    PartialPath getPartialPath();

    IMNode getParent();

    void setParent(IMNode parent);

    Map<String, IMNode> getChildren();

    Map<String, IMNode> getAliasChildren();

    void setChildren(Map<String, IMNode> children);

    void setAliasChildren(Map<String, IMNode> aliasChildren);

    String getName();

    void setName(String name);

    void serializeTo(MLogWriter logWriter) throws IOException;

    void replaceChild(String measurement, IMNode newChildNode);

    void setFullPath(String fullPath);

    Template getUpperTemplate();

    boolean isUseTemplate();

    void setUseTemplate(boolean useTemplate);
}
