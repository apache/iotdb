package org.apache.iotdb.influxdb.utils;

import org.apache.iotdb.influxdb.protocol.util.DataTypeUtils;
import org.influxdb.dto.Point;
import org.junit.Assert;
import org.junit.Test;

public class DataTypeUtilsTest {
    @Test
    public void recordToPointTest() {
        String[] records = {"student,name=xie,sex=m country=\"china\",score=87.0,tel=\"110\" 1635177018815000000",
                "student,name=xie,sex=m country=\"china\",score=87i,tel=990i 1635187018815000000",
                "cpu,name=xie country=\"china\",score=100.0 1635187018815000000"};
        int expectLength = 3;
        for (int i = 0; i < expectLength; i++) {
            Assert.assertEquals(records[i],DataTypeUtils.recordToPoint(records[i], null).lineProtocol());
        }
    }
}
