/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.qp.plan;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.qp.physical.crud.AggregationPlan;
import org.apache.iotdb.db.qp.physical.crud.FillQueryPlan;
import org.apache.iotdb.db.qp.physical.crud.GroupByPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.query.fill.LinearFill;
import org.apache.iotdb.db.query.fill.PreviousFill;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.BinaryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.ValueFilter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;
import org.apache.iotdb.db.qp.utils.MemIntQpExecutor;
import org.junit.Before;
import org.junit.Test;

import org.apache.iotdb.db.exception.ArgsErrorException;
import org.apache.iotdb.db.qp.QueryProcessor;
import org.apache.iotdb.db.exception.qp.QueryProcessorException;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.AuthorPlan;
import org.apache.iotdb.db.qp.physical.sys.MetadataPlan;
import org.apache.iotdb.db.qp.physical.sys.PropertyPlan;
import org.apache.iotdb.db.qp.utils.MemIntQpExecutor;
import org.apache.iotdb.tsfile.common.constant.SystemConstant;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.StringContainer;

public class PhysicalPlanTest {

    private QueryProcessor processor = new QueryProcessor(new MemIntQpExecutor());

    @Before
    public void before() throws ProcessorException {
        Path path1 = new Path(
                new StringContainer(new String[] { "root", "vehicle", "d1", "s1" }, SystemConstant.PATH_SEPARATOR));
        Path path2 = new Path(
                new StringContainer(new String[] { "root", "vehicle", "d2", "s1" }, SystemConstant.PATH_SEPARATOR));
        Path path3 = new Path(
                new StringContainer(new String[] { "root", "vehicle", "d3", "s1" }, SystemConstant.PATH_SEPARATOR));
        Path path4 = new Path(
                new StringContainer(new String[] { "root", "vehicle", "d4", "s1" }, SystemConstant.PATH_SEPARATOR));
        processor.getExecutor().insert(path1, 10, "10");
        processor.getExecutor().insert(path2, 10, "10");
        processor.getExecutor().insert(path3, 10, "10");
        processor.getExecutor().insert(path4, 10, "10");
    }

    @Test
    public void testMetadata() throws QueryProcessorException, ArgsErrorException, ProcessorException {
        String metadata = "create timeseries root.vehicle.d1.s1 with datatype=INT32,encoding=RLE";
        QueryProcessor processor = new QueryProcessor(new MemIntQpExecutor());
        MetadataPlan plan = (MetadataPlan) processor.parseSQLToPhysicalPlan(metadata);
        assertEquals("seriesPath: root.vehicle.d1.s1\n" + "dataType: INT32\n" + "encoding: RLE\n"
                + "namespace type: ADD_PATH\n" + "args: ", plan.toString());
    }

    @Test
    public void testAuthor() throws QueryProcessorException, ArgsErrorException, ProcessorException {
        String sql = "grant role xm privileges 'SET_STORAGE_GROUP','DELETE_TIMESERIES' on root.vehicle.d1.s1";
        QueryProcessor processor = new QueryProcessor(new MemIntQpExecutor());
        AuthorPlan plan = (AuthorPlan) processor.parseSQLToPhysicalPlan(sql);
        assertEquals(
                "userName: null\n" + "roleName: xm\n" + "password: null\n" + "newPassword: null\n"
                        + "permissions: [0, 4]\n" + "nodeName: root.vehicle.d1.s1\n" + "authorType: GRANT_ROLE",
                plan.toString());
    }

    @Test
    public void testProperty() throws QueryProcessorException, ArgsErrorException, ProcessorException {
        String sql = "add label label1021 to property propropro";
        QueryProcessor processor = new QueryProcessor(new MemIntQpExecutor());
        PropertyPlan plan = (PropertyPlan) processor.parseSQLToPhysicalPlan(sql);
        assertEquals(
                "propertyPath: propropro.label1021\n" + "metadataPath: null\n" + "propertyType: ADD_PROPERTY_LABEL",
                plan.toString());
    }

    // TODO uncomment these code when implement aggregation and fill function

    @Test
    public void testAggregation() throws QueryProcessorException, ArgsErrorException, ProcessorException {
        String sqlStr = "select sum(d1.s1) " + "from root.vehicle "
                + "where time <= 51 or !(time != 100 and time < 460)";
        PhysicalPlan plan = processor.parseSQLToPhysicalPlan(sqlStr);
        if (!plan.isQuery())
            fail();
        AggregationPlan mergePlan = (AggregationPlan) plan;
        assertEquals("sum", mergePlan.getAggregations().get(0));
    }

    @Test
    public void testGroupBy1() throws QueryProcessorException, ArgsErrorException, ProcessorException {
        String sqlStr = "select count(s1) " + "from root.vehicle.d1 " + "where s1 < 20 and time <= now() "
                + "group by(10m, 44, [1,3], [4,5])";
        PhysicalPlan plan = processor.parseSQLToPhysicalPlan(sqlStr);
        if (!plan.isQuery())
            fail();
        GroupByPlan mergePlan = (GroupByPlan) plan;
        assertEquals(44, mergePlan.getOrigin());
    }

    @Test
    public void testGroupBy2() throws QueryProcessorException, ArgsErrorException, ProcessorException {
        String sqlStr = "select count(s1) " + "from root.vehicle.d1 " + "where s1 < 20 and time <= now() "
                + "group by(111ms, [123,2017-6-2T12:00:12+07:00], [55555, now()])";
        PhysicalPlan plan = processor.parseSQLToPhysicalPlan(sqlStr);
        if (!plan.isQuery())
            fail();
        GroupByPlan mergePlan = (GroupByPlan) plan;
        assertEquals(111, mergePlan.getUnit());
    }

    @Test
    public void testFill1() throws QueryProcessorException, ArgsErrorException, ProcessorException {
        String sqlStr = "SELECT s1 FROM root.vehicle.d1 WHERE time = 5000 Fill(int32[linear, 5m, 5m], boolean[previous, 5m])";
        PhysicalPlan plan = processor.parseSQLToPhysicalPlan(sqlStr);
        if (!plan.isQuery())
            fail();
        FillQueryPlan mergePlan = (FillQueryPlan) plan;
        assertEquals(5000, mergePlan.getQueryTime());
        assertEquals(300000, ((LinearFill) mergePlan.getFillType().get(TSDataType.INT32)).getBeforeRange());
        assertEquals(300000, ((LinearFill) mergePlan.getFillType().get(TSDataType.INT32)).getAfterRange());
        assertEquals(300000, ((PreviousFill) mergePlan.getFillType().get(TSDataType.BOOLEAN)).getBeforeRange());
    }

    @Test
    public void testFill2() throws QueryProcessorException, ArgsErrorException, ProcessorException {
        String sqlStr = "SELECT s1 FROM root.vehicle.d1 WHERE time = 5000 Fill(int32[linear], boolean[previous])";
        PhysicalPlan plan = processor.parseSQLToPhysicalPlan(sqlStr);
        if (!plan.isQuery())
            fail();
        FillQueryPlan mergePlan = (FillQueryPlan) plan;
        assertEquals(5000, mergePlan.getQueryTime());
        assertEquals(-1, ((LinearFill) mergePlan.getFillType().get(TSDataType.INT32)).getBeforeRange());
        assertEquals(-1, ((LinearFill) mergePlan.getFillType().get(TSDataType.INT32)).getAfterRange());
        assertEquals(-1, ((PreviousFill) mergePlan.getFillType().get(TSDataType.BOOLEAN)).getBeforeRange());
    }

    @Test
    public void testFill3() throws QueryProcessorException, ArgsErrorException {
        String sqlStr = "SELECT s1 FROM root.vehicle.d1 WHERE time = 5000 Fill(int32[linear, 5m], boolean[previous])";
        try {
            PhysicalPlan plan = processor.parseSQLToPhysicalPlan(sqlStr);
        } catch (Exception e) {
            assertTrue(true);
        }
    }

    @Test
    public void testFill4() throws QueryProcessorException, ArgsErrorException {
        String sqlStr = "SELECT s1 FROM root.vehicle.d1 WHERE time > 5000 Fill(int32[linear], boolean[previous])";
        try {
            PhysicalPlan plan = processor.parseSQLToPhysicalPlan(sqlStr);
        } catch (Exception e) {
            assertEquals("Only \"=\" can be used in fill function", e.getMessage().toString());
        }
    }

    @Test
    public void testQuery1() throws QueryProcessorException, ArgsErrorException, ProcessorException {
        String sqlStr = "SELECT s1 FROM root.vehicle.d1 WHERE time > 5000";
        PhysicalPlan plan = processor.parseSQLToPhysicalPlan(sqlStr);
        IExpression queryFilter = ((QueryPlan) plan).getExpression();
        IExpression expect = new GlobalTimeExpression(TimeFilter.gt(5000L));
        assertEquals(expect.toString(), queryFilter.toString());
    }

    @Test
    public void testQuery2() throws QueryProcessorException, ArgsErrorException, ProcessorException {
        String sqlStr = "SELECT s1 FROM root.vehicle.d1 WHERE time > 50 and time <= 100";
        PhysicalPlan plan = processor.parseSQLToPhysicalPlan(sqlStr);
        IExpression queryFilter = ((QueryPlan) plan).getExpression();
        IExpression expect = new GlobalTimeExpression(FilterFactory.and(TimeFilter.gt(50L), TimeFilter.ltEq(100L)));
        assertEquals(expect.toString(), queryFilter.toString());

    }

    @Test
    public void testQuery3() throws QueryProcessorException, ArgsErrorException, ProcessorException {
        String sqlStr = "SELECT s1 FROM root.vehicle.d1 WHERE time > 50 and time <= 100 or s1 < 10";
        PhysicalPlan plan = processor.parseSQLToPhysicalPlan(sqlStr);
        IExpression queryFilter = ((QueryPlan) plan).getExpression();
        IExpression expect = new GlobalTimeExpression(FilterFactory.and(TimeFilter.gt(50L), TimeFilter.ltEq(100L)));
        expect = BinaryExpression.or(expect,
                new SingleSeriesExpression(new Path("root.vehicle.d1.s1"), ValueFilter.lt(10)));
        assertEquals(expect.toString(), queryFilter.toString());
    }

    @Test
    public void testQuery4() throws QueryProcessorException, ArgsErrorException, ProcessorException {
        String sqlStr = "SELECT s1 FROM root.vehicle.d1 WHERE time > 50 and time <= 100 and s1 < 10";
        PhysicalPlan plan = processor.parseSQLToPhysicalPlan(sqlStr);
        IExpression queryFilter = ((QueryPlan) plan).getExpression();

        IExpression expect = BinaryExpression.and(
                new SingleSeriesExpression(new Path("root.vehicle.d1.s1"), ValueFilter.lt(10)),
                new GlobalTimeExpression(FilterFactory.and(TimeFilter.gt(50L), TimeFilter.ltEq(100L))));

        assertEquals(expect.toString(), queryFilter.toString());

        Path path = new Path("root.vehicle.d1.s1");
        assertEquals(path, plan.getPaths().get(0));
    }

    @Test
    public void testQuery5() throws QueryProcessorException, ArgsErrorException, ProcessorException {
        String sqlStr = "SELECT s1 FROM root.vehicle.d1 WHERE s1 > 20 or s1 < 10";
        PhysicalPlan plan = processor.parseSQLToPhysicalPlan(sqlStr);
        IExpression queryFilter = ((QueryPlan) plan).getExpression();
        IExpression expect = new SingleSeriesExpression(new Path("root.vehicle.d1.s1"),
                FilterFactory.or(ValueFilter.gt(20), ValueFilter.lt(10)));
        assertEquals(expect.toString(), queryFilter.toString());

    }

    @Test
    public void testQuery6() throws QueryProcessorException, ArgsErrorException, ProcessorException {
        String sqlStr = "SELECT s1 FROM root.vehicle.d1 WHERE time > 20 or time < 10";
        PhysicalPlan plan = processor.parseSQLToPhysicalPlan(sqlStr);
        IExpression queryFilter = ((QueryPlan) plan).getExpression();
        IExpression expect = new GlobalTimeExpression(FilterFactory.or(TimeFilter.gt(20L), TimeFilter.lt(10L)));
        assertEquals(expect.toString(), queryFilter.toString());

    }

}
