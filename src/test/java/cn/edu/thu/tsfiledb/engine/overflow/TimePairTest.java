package cn.edu.thu.tsfiledb.engine.overflow;

import cn.edu.thu.tsfile.common.utils.BytesUtils;
import cn.edu.thu.tsfiledb.engine.overflow.utils.OverflowOpType;
import cn.edu.thu.tsfiledb.engine.overflow.utils.TimePair;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author CGF.
 */
public class TimePairTest {

    @Test
    public void timePairTest() {
        TimePair tp1 = new TimePair(10L, 20L);
        Assert.assertEquals(tp1.s, 10L);
        Assert.assertEquals(tp1.e, 20L);
        Assert.assertEquals(tp1.toString(), "10,20");

        TimePair tp2 = new TimePair(15L, 25L, BytesUtils.intToBytes(10), OverflowOpType.UPDATE);
        Assert.assertEquals(tp2.s, 15L);
        Assert.assertEquals(tp2.e, 25L);
        Assert.assertEquals(tp2.toString(), "15,25,UPDATE");

        TimePair tp3 = new TimePair(20L, 30L, BytesUtils.floatToBytes(10.0f), OverflowOpType.DELETE);
        Assert.assertEquals(tp3.s, 20L);
        Assert.assertEquals(tp3.e, 30L);
        Assert.assertEquals(tp3.toString(), "20,30,DELETE");
    }
}
