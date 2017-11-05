package cn.edu.tsinghua.iotdb.qp.logical.sys;

import cn.edu.tsinghua.iotdb.qp.logical.RootOperator;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;

/**
 * this class maintains information in Metadata.namespace statement
 * 
 * @author kangrong
 *
 */
public class PropertyOperator extends RootOperator {

    public PropertyOperator(int tokenIntType, PropertyType type) {
        super(tokenIntType);
        propertyType = type;
        operatorType = OperatorType.PROPERTY;
    }

    private final PropertyType propertyType;
    private Path propertyPath;
    private Path metadataPath;

    public Path getPropertyPath() {
        return propertyPath;
    }

    public void setPropertyPath(Path propertyPath) {
        this.propertyPath = propertyPath;
    }

    public Path getMetadataPath() {
        return metadataPath;
    }

    public void setMetadataPath(Path metadataPath) {
        this.metadataPath = metadataPath;
    }

    public PropertyType getPropertyType() {
        return propertyType;
    }

    public enum PropertyType {
        ADD_TREE, ADD_PROPERTY_LABEL, DELETE_PROPERTY_LABEL, ADD_PROPERTY_TO_METADATA,DEL_PROPERTY_FROM_METADATA
    }
}
