package org.apache.iotdb.db.qp.other;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collection;

import org.apache.iotdb.db.exception.ArgsErrorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.qp.utils.MemIntQpExecutor;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import org.apache.iotdb.db.qp.QueryProcessor;
import org.apache.iotdb.db.exception.qp.QueryProcessorException;
import org.apache.iotdb.db.qp.logical.sys.PropertyOperator.PropertyType;
import org.apache.iotdb.db.qp.physical.sys.PropertyPlan;
import org.apache.iotdb.db.qp.utils.MemIntQpExecutor;
import org.apache.iotdb.tsfile.read.common.Path;

/**
 * test ast node parsing on authorization
 * 
 * @author kangrong
 *
 */
@RunWith(Parameterized.class)
public class TSPlanContextPropertyTest {
    private static Path defaultMetadataPath = new Path("root.m1.m2");
    private static Path defaultPropertyPath = new Path("property1");
    private static Path defaultPropertyLabelPath = new Path("property1.label1");

    private String inputSQL;
    private PropertyType propertyType;
    private Path propertyPath;
    private Path metadataPath;
    private Path[] paths;


    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {"CREATE PROPERTY property1", PropertyType.ADD_TREE, defaultPropertyPath, null,
                        new Path[] {defaultPropertyPath}},
                {"ADD LABEL label1 TO PROPERTY property1", PropertyType.ADD_PROPERTY_LABEL,
                        defaultPropertyLabelPath, null, new Path[] {defaultPropertyLabelPath}},
                {"DELETE LABEL label1 FROM PROPERTY property1", PropertyType.DELETE_PROPERTY_LABEL,
                        defaultPropertyLabelPath, null, new Path[] {defaultPropertyLabelPath}},
                {"LINK root.m1.m2 TO property1.label1", PropertyType.ADD_PROPERTY_TO_METADATA,
                        defaultPropertyLabelPath, defaultMetadataPath,
                        new Path[] {defaultMetadataPath, defaultPropertyLabelPath}},
                {"UNLINK root.m1.m2 FROM property1.label1",
                        PropertyType.DEL_PROPERTY_FROM_METADATA, defaultPropertyLabelPath,
                        defaultMetadataPath,
                        new Path[] {defaultMetadataPath, defaultPropertyLabelPath}},});
    }

    public TSPlanContextPropertyTest(String inputSQL, PropertyType propertyType, Path propertyPath,
            Path metadataPath, Path[] paths) {
        this.inputSQL = inputSQL;
        this.propertyType = propertyType;
        this.propertyPath = propertyPath;
        this.metadataPath = metadataPath;
        this.paths = paths;
    }

    @Test
    public void testAnalyzeMetadata() throws QueryProcessorException, ArgsErrorException, ProcessorException {
        QueryProcessor processor = new QueryProcessor(new MemIntQpExecutor());
        PropertyPlan plan = (PropertyPlan) processor.parseSQLToPhysicalPlan(inputSQL);
        assertEquals(propertyType, plan.getPropertyType());
        assertEquals(propertyPath, plan.getPropertyPath());
        assertEquals(metadataPath, plan.getMetadataPath());
        assertArrayEquals(paths, plan.getPaths().toArray());
    }
}
