package cn.edu.thu.tsfiledb.sql.exec.utils;

import cn.edu.thu.tsfiledb.qp.exception.QueryProcessorException;
import cn.edu.thu.tsfiledb.qp.logical.operator.crud.FilterOperator;
import cn.edu.thu.tsfiledb.qp.logical.operator.crud.SFWOperator;
import cn.edu.thu.tsfiledb.qp.physical.plan.query.MergeQuerySetPlan;
import cn.edu.thu.tsfiledb.sql.exec.TSqlParserV2;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class FilterCloneTest {
    private String sql;

    public FilterCloneTest(String sql) {
        this.sql = sql;
    }

    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
        {"select time where delta_object > 1 and (delta_object = d1 or delta_object = d2)"
                + " and (delta_object = d2 or delta_object = d3)"}
        });
    }

    @Test
    public void testMergeDeltaObject() {
        // String inputSQL = "select time where delta_object = d1 or delta_object = d2";
        TSqlParserV2 parser = new TSqlParserV2();
        try {
            SFWOperator root = (SFWOperator) parser.parseSQLToOperator(sql);
            root = parser.logicalOptimize(root);
            MergeQuerySetPlan plan =
                    (MergeQuerySetPlan) root.transformToPhysicalPlan(new MemIntQpExecutor());
            FilterOperator filter = plan.getSelectPlans().get(0).getDeltaObjectFilterOperator();
            FilterOperator copy = filter.clone();
            filter.addHeadDeltaObjectPath("asd");
            copy.addHeadDeltaObjectPath("asd");
             assertEquals(filter.showTree(), copy.showTree());
        } catch (QueryProcessorException e) {
            e.printStackTrace();
            fail();
        }


    }

}
