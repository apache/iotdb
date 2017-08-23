package cn.edu.thu.tsfiledb.qp.physical.sys;

import java.util.ArrayList;
import java.util.List;

import cn.edu.thu.tsfiledb.qp.logical.Operator.OperatorType;
import cn.edu.thu.tsfiledb.qp.logical.sys.PropertyOperator.PropertyType;
import cn.edu.thu.tsfiledb.qp.physical.PhysicalPlan;
import cn.edu.tsinghua.tsfile.timeseries.read.qp.Path;

/**
 * Manipulate property plan
 * @author kangrong
 * @author qiaojialin
 */
public class PropertyPlan extends PhysicalPlan {
    private final PropertyType propertyType;
    private Path propertyPath;
    private Path metadataPath;

    public Path getPropertyPath() {
        return propertyPath;
    }

    public Path getMetadataPath() {
        return metadataPath;
    }

    public PropertyType getPropertyType() {
        return propertyType;
    }

    
    public PropertyPlan(PropertyType propertyType, Path propertyPath, Path metadataPath) {
        super(false, OperatorType.PROPERTY);
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
        if (metadataPath != null)
            ret.add(metadataPath);
        if (propertyPath != null)
            ret.add(propertyPath);
        return ret;
    }
}
