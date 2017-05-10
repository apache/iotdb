package cn.edu.thu.tsfiledb.qp.physical.plan.metadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import cn.edu.thu.tsfile.common.exception.ProcessorException;
import cn.edu.thu.tsfiledb.exception.ArgsErrorException;
import cn.edu.thu.tsfiledb.exception.PathErrorException;
import cn.edu.thu.tsfiledb.metadata.MManager;
import cn.edu.thu.tsfiledb.qp.exception.physical.plan.NamespacePlanException;
import cn.edu.thu.tsfiledb.qp.exception.physical.plan.PhysicalPlanException;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfiledb.qp.exec.QueryProcessExecutor;
import cn.edu.thu.tsfiledb.qp.logical.operator.Operator.OperatorType;
import cn.edu.thu.tsfiledb.qp.logical.operator.metadata.PropertyOperator.PropertyType;
import cn.edu.thu.tsfiledb.qp.physical.plan.PhysicalPlan;

/**
 * given a author related plan and construct a {@code AuthorPlan}
 * 
 * @author kangrong
 *
 */
public class PropertyPlan extends PhysicalPlan {
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

    
    public PropertyPlan(PropertyType propertyType, Path propertyPath, Path metadataPath) {
        super(false, OperatorType.PROPERTY);
        this.propertyType = propertyType;
        this.propertyPath = propertyPath;
        this.metadataPath = metadataPath;
    }

    public boolean processNonQuery(QueryProcessExecutor config) throws ProcessorException {
        MManager mManager = config.getMManager();
        try {
            switch (propertyType) {
                case ADD_TREE:
                    mManager.addAPTree(propertyPath.getFullPath());
                    break;
                case ADD_PROPERTY_LABEL:
                    mManager.addAPathToPTree(propertyPath.getFullPath());
                    break;
                case DELETE_PROPERTY_LABEL:
                    mManager.deletePathFromPTree(propertyPath.getFullPath());
                    break;
                case ADD_PROPERTY_TO_METADATA:
                    mManager.linkMNodeToPTree(propertyPath.getFullPath(), metadataPath.getFullPath());
                    break;
                case DEL_PROPERTY_FROM_METADATA:
                    mManager.unlinkMNodeFromPTree(propertyPath.getFullPath(), metadataPath.getFullPath());
                    break;
                default:
                    throw new ProcessorException("unkown namespace type:" + propertyType);
            }
        } catch (PathErrorException | IOException | ArgsErrorException e) {
            throw new ProcessorException("meet err in " + propertyType +" . "+ e.getMessage());
        }
        return true;
    }
    @Override
    public List<Path> getInvolvedSeriesPaths() {
        List<Path> ret = new ArrayList<Path>();
        if (metadataPath != null)
            ret.add(metadataPath);
        if (propertyPath != null)
            ret.add(propertyPath);
        return ret;
    }
}
