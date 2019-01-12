package org.apache.iotdb.db.qp.physical.sys;

import java.util.ArrayList;
import java.util.List;

import org.apache.iotdb.db.qp.logical.sys.PropertyOperator.PropertyType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.logical.sys.PropertyOperator;

/**
 * Manipulate property plan
 */
public class PropertyPlan extends PhysicalPlan {
    private final PropertyOperator.PropertyType propertyType;
    private Path propertyPath;
    private Path metadataPath;

    public Path getPropertyPath() {
        return propertyPath;
    }

    public Path getMetadataPath() {
        return metadataPath;
    }

    public PropertyOperator.PropertyType getPropertyType() {
        return propertyType;
    }

    
    public PropertyPlan(PropertyOperator.PropertyType propertyType, Path propertyPath, Path metadataPath) {
        super(false, Operator.OperatorType.PROPERTY);
        this.propertyType = propertyType;
        this.propertyPath = propertyPath;
        this.metadataPath = metadataPath;
    }

    @Override
    public String toString() {
        return "propertyPath: "+ propertyPath.toString() +
                "\nmetadataPath: " + metadataPath +
                "\npropertyType: " + propertyType;
    }

    @Override
    public List<Path> getPaths() {
        List<Path> ret = new ArrayList<>();
        if (metadataPath != null) {
            ret.add(metadataPath);
        }
        if (propertyPath != null) {
            ret.add(propertyPath);
        }
        return ret;
    }
}
