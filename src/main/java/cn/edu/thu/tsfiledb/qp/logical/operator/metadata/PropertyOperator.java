package cn.edu.thu.tsfiledb.qp.logical.operator.metadata;

import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfiledb.qp.exception.QueryProcessorException;
import cn.edu.thu.tsfiledb.qp.exec.QueryProcessExecutor;
import cn.edu.thu.tsfiledb.qp.logical.operator.RootOperator;
import cn.edu.thu.tsfiledb.qp.physical.plan.PhysicalPlan;
import cn.edu.thu.tsfiledb.qp.physical.plan.metadata.PropertyPlan;

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

    @Override
    public PhysicalPlan transformToPhysicalPlan(QueryProcessExecutor conf)
            throws QueryProcessorException {
        return new PropertyPlan(propertyType, propertyPath, metadataPath);
    }

    public static enum PropertyType {
        ADD_TREE, ADD_PROPERTY_LABEL, DELETE_PROPERTY_LABEL, ADD_PROPERTY_TO_METADATA,DEL_PROPERTY_FROM_METADATA
    }
}
