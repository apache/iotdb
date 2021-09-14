package org.apache.iotdb.influxdb;

import org.apache.iotdb.infludb.IotDBInfluxDBUtils;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.SessionDataSet;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

public class IotDBInfluxDBUtilsTest {

    @Test
    public void testGetSame() throws IoTDBConnectionException, StatementExecutionException {//测试数据查询

        ArrayList<String> columns = new ArrayList<>();
        columns.addAll(Arrays.asList("time", "root.111.1", "root.111.2", "root.222.1"));
        ArrayList<Integer> list = IotDBInfluxDBUtils.getSamePathForList(columns.subList(1,columns.size()));
        assert list.get(0) == 1;
        assert list.get(1) == 3;
        assert list.size() == 2;
    }

}
