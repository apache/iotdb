package org.apache.iotdb.tsfile.encoding;

import org.junit.Test;

import java.io.IOException;

import org.apache.iotdb.tsfile.encoding.SubcolumnAblationPruneNewEngine.Mode;

public class SubcolumnWithoutRLETest {

    @Test
    public void test0() throws IOException {
        SubcolumnAblationPruneNewEngine.runAblationBenchmark(
                "/Users/xiaojinzhao/Documents/GitHub/subcolumn/result/subcolumn_without_rle.csv", Mode.WITHOUT_RLE);
    }
}
