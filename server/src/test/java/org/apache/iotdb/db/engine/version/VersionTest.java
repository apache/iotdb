package org.apache.iotdb.db.engine.version;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.exception.*;
import org.apache.iotdb.db.qp.executor.QueryProcessExecutor;
import org.apache.iotdb.db.qp.physical.sys.ShowVersionPlan;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * @Author: LiuDaWei
 * @Create: 2019年11月01日
 */
public class VersionTest {
    private String sg1 = IoTDBConstant.VERSION;

    @Before
    public void setUp()
            throws MetadataErrorException, ProcessorException, IOException, StartupException, PathErrorException {
        EnvironmentUtils.envSetUp();
    }

    @After
    public void tearDown() throws IOException, StorageEngineException {
        EnvironmentUtils.cleanEnv();
    }

    @Test
    public void testShowVersion() throws ProcessorException, QueryFilterOptimizationException, PathErrorException,
            StorageEngineException, IOException {
        ShowVersionPlan plan = new ShowVersionPlan();
        QueryProcessExecutor executor = new QueryProcessExecutor();
        QueryDataSet queryDataSet = executor.processQuery(plan, EnvironmentUtils.TEST_QUERY_CONTEXT);

        RowRecord rowRecord = queryDataSet.next();
        assertEquals(sg1, rowRecord.getFields().get(0).getStringValue());
    }
}
