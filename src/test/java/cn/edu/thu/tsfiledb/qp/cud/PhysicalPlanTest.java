package cn.edu.thu.tsfiledb.qp.cud;

import cn.edu.thu.tsfiledb.exception.ArgsErrorException;
import cn.edu.thu.tsfiledb.qp.QueryProcessor;
import cn.edu.thu.tsfiledb.qp.exception.QueryProcessorException;
import cn.edu.thu.tsfiledb.qp.physical.sys.AuthorPlan;
import cn.edu.thu.tsfiledb.qp.physical.sys.MetadataPlan;
import cn.edu.thu.tsfiledb.qp.physical.sys.PropertyPlan;
import cn.edu.thu.tsfiledb.qp.utils.MemIntQpExecutor;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class PhysicalPlanTest {
    @Test
    public void testMetadata() throws QueryProcessorException, ArgsErrorException {
        String metadata = "create timeseries root.laptop.d1.s1 with datatype=INT32,encoding=RLE";
        QueryProcessor processor = new QueryProcessor(new MemIntQpExecutor());
        MetadataPlan plan = (MetadataPlan)processor.parseSQLToPhysicalPlan(metadata);
        assertEquals("path: root.laptop.d1.s1\n" +
                "dataType: INT32\n" +
                "encoding: RLE\n" +
                "namespace type: ADD_PATH\n" +
                "args: " , plan.toString());
    }

    @Test
    public void testAuthor() throws QueryProcessorException, ArgsErrorException {
        String sql = "grant role xm privileges 'create','delete' on root.laptop.d1.s1";
        QueryProcessor processor = new QueryProcessor(new MemIntQpExecutor());
        AuthorPlan plan = (AuthorPlan) processor.parseSQLToPhysicalPlan(sql);
        assertEquals("userName: null\n" +
                "roleName: xm\n" +
                "password: null\n" +
                "newPassword: null\n" +
                "permissions: [0, 4]\n" +
                "nodeName: root.laptop.d1.s1\n" +
                "authorType: GRANT_ROLE", plan.toString());
    }

    @Test
    public void testProperty() throws QueryProcessorException, ArgsErrorException {
        String sql = "add label label1021 to property propropro";
        QueryProcessor processor = new QueryProcessor(new MemIntQpExecutor());
        PropertyPlan plan = (PropertyPlan) processor.parseSQLToPhysicalPlan(sql);
        assertEquals("propertyPath: propropro.label1021\n" +
                "metadataPath: null\n" +
                "propertyType: ADD_PROPERTY_LABEL", plan.toString());
    }

}
