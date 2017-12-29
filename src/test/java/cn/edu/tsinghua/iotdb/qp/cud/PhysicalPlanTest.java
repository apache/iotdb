package cn.edu.tsinghua.iotdb.qp.cud;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import cn.edu.tsinghua.iotdb.qp.physical.crud.MultiQueryPlan;
import cn.edu.tsinghua.iotdb.query.fill.LinearFill;
import cn.edu.tsinghua.iotdb.query.fill.PreviousFill;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import org.junit.Before;
import org.junit.Test;

import cn.edu.tsinghua.iotdb.exception.ArgsErrorException;
import cn.edu.tsinghua.iotdb.qp.QueryProcessor;
import cn.edu.tsinghua.iotdb.qp.exception.QueryProcessorException;
import cn.edu.tsinghua.iotdb.qp.physical.PhysicalPlan;
import cn.edu.tsinghua.iotdb.qp.physical.sys.AuthorPlan;
import cn.edu.tsinghua.iotdb.qp.physical.sys.MetadataPlan;
import cn.edu.tsinghua.iotdb.qp.physical.sys.PropertyPlan;
import cn.edu.tsinghua.iotdb.qp.utils.MemIntQpExecutor;
import cn.edu.tsinghua.tsfile.common.constant.SystemConstant;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.utils.StringContainer;

public class PhysicalPlanTest {

    private QueryProcessor processor = new QueryProcessor(new MemIntQpExecutor());

    @Before
    public void before() throws ProcessorException {
        Path path1 =
                new Path(new StringContainer(
                        new String[] {"root", "vehicle", "d1", "s1"},
                        SystemConstant.PATH_SEPARATOR));
        Path path2 =
                new Path(new StringContainer(
                        new String[] {"root", "vehicle", "d2", "s1"},
                        SystemConstant.PATH_SEPARATOR));
        Path path3 =
                new Path(new StringContainer(
                        new String[] {"root", "vehicle", "d3", "s1"},
                        SystemConstant.PATH_SEPARATOR));
        Path path4 =
                new Path(new StringContainer(
                        new String[] {"root", "vehicle", "d4", "s1"},
                        SystemConstant.PATH_SEPARATOR));
        processor.getExecutor().insert(path1, 10, "10");
        processor.getExecutor().insert(path2, 10, "10");
        processor.getExecutor().insert(path3, 10, "10");
        processor.getExecutor().insert(path4, 10, "10");
    }

    @Test
    public void testMetadata() throws QueryProcessorException, ArgsErrorException {
        String metadata = "create timeseries root.vehicle.d1.s1 with datatype=INT32,encoding=RLE";
        QueryProcessor processor = new QueryProcessor(new MemIntQpExecutor());
        MetadataPlan plan = (MetadataPlan)processor.parseSQLToPhysicalPlan(metadata);
        assertEquals("path: root.vehicle.d1.s1\n" +
                "dataType: INT32\n" +
                "encoding: RLE\n" +
                "namespace type: ADD_PATH\n" +
                "args: " , plan.toString());
    }

    @Test
    public void testAuthor() throws QueryProcessorException, ArgsErrorException {
        String sql = "grant role xm privileges 'create','delete' on root.vehicle.d1.s1";
        QueryProcessor processor = new QueryProcessor(new MemIntQpExecutor());
        AuthorPlan plan = (AuthorPlan) processor.parseSQLToPhysicalPlan(sql);
        assertEquals("userName: null\n" +
                "roleName: xm\n" +
                "password: null\n" +
                "newPassword: null\n" +
                "permissions: [0, 4]\n" +
                "nodeName: root.vehicle.d1.s1\n" +
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

    @Test
    public void testAggregation() throws QueryProcessorException, ArgsErrorException {
        String sqlStr =
                "select sum(d1.s1) " + "from root.vehicle "
                        + "where time <= 51 or !(time != 100 and time < 460)";
        PhysicalPlan plan = processor.parseSQLToPhysicalPlan(sqlStr);
        if (!plan.isQuery())
            fail();
        MultiQueryPlan mergePlan = (MultiQueryPlan) plan;
        assertEquals("sum", mergePlan.getAggregations().get(0));
    }

    @Test
    public void testGroupBy1() throws QueryProcessorException, ArgsErrorException {
        String sqlStr =
                "select count(s1) "
                        + "from root.vehicle.d1 "
                        + "where s1 < 20 and time <= now() "
                        + "group by(10m, 44, [1,3], [4,5])";
        PhysicalPlan plan = processor.parseSQLToPhysicalPlan(sqlStr);
        if (!plan.isQuery())
            fail();
        MultiQueryPlan mergePlan = (MultiQueryPlan) plan;
        assertEquals(44, mergePlan.getOrigin());
    }

    @Test
    public void testGroupBy2() throws QueryProcessorException, ArgsErrorException {
        String sqlStr =
                "select count(s1) "
                        + "from root.vehicle.d1 "
                        + "where s1 < 20 and time <= now() "
                        + "group by(111ms, [123,2017-6-2T12:00:12+07:00], [55555, now()])";
        PhysicalPlan plan = processor.parseSQLToPhysicalPlan(sqlStr);
        if (!plan.isQuery())
            fail();
        MultiQueryPlan mergePlan = (MultiQueryPlan) plan;
        assertEquals(111, mergePlan.getUnit());
    }

    @Test
    public void testFill1() throws QueryProcessorException, ArgsErrorException {
        String sqlStr =
                "SELECT s1 FROM root.vehicle.d1 WHERE time = 5000 Fill(int32[linear, 5m, 5m], boolean[previous, 5m])";
        PhysicalPlan plan = processor.parseSQLToPhysicalPlan(sqlStr);
        if (!plan.isQuery())
            fail();
        MultiQueryPlan mergePlan = (MultiQueryPlan) plan;
        assertEquals(5000, mergePlan.getQueryTime());
        assertEquals(300000, ((LinearFill)mergePlan.getFillType().get(TSDataType.INT32)).getBeforeRange());
        assertEquals(300000, ((LinearFill)mergePlan.getFillType().get(TSDataType.INT32)).getAfterRange());
        assertEquals(300000, ((PreviousFill)mergePlan.getFillType().get(TSDataType.BOOLEAN)).getBeforeRange());
    }

    @Test
    public void testFill2() throws QueryProcessorException, ArgsErrorException {
        String sqlStr =
                "SELECT s1 FROM root.vehicle.d1 WHERE time = 5000 Fill(int32[linear], boolean[previous])";
        PhysicalPlan plan = processor.parseSQLToPhysicalPlan(sqlStr);
        if (!plan.isQuery())
            fail();
        MultiQueryPlan mergePlan = (MultiQueryPlan) plan;
        assertEquals(5000, mergePlan.getQueryTime());
        assertEquals(-1, ((LinearFill)mergePlan.getFillType().get(TSDataType.INT32)).getBeforeRange());
        assertEquals(-1, ((LinearFill)mergePlan.getFillType().get(TSDataType.INT32)).getAfterRange());
        assertEquals(-1, ((PreviousFill)mergePlan.getFillType().get(TSDataType.BOOLEAN)).getBeforeRange());
    }

    @Test
    public void testFill3() throws QueryProcessorException, ArgsErrorException {
        String sqlStr =
                "SELECT s1 FROM root.vehicle.d1 WHERE time = 5000 Fill(int32[linear, 5m], boolean[previous])";
        try {
            PhysicalPlan plan = processor.parseSQLToPhysicalPlan(sqlStr);
        } catch (Exception e) {
            assertTrue(true);
        }
    }

}
