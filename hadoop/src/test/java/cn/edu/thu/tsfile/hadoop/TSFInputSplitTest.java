package cn.edu.thu.tsfile.hadoop;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.junit.Before;
import org.junit.Test;

import cn.edu.tsinghua.tsfile.file.metadata.RowGroupMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkMetaData;

/**
 * Test the {@link cn.edu.thu.tsfile.hadoop.TSFInputSplit}
 * Assert the readFields function and write function is right
 *
 * @author liukun
 */
public class TSFInputSplitTest {

    private TSFInputSplit wInputSplit;
    private TSFInputSplit rInputSplit;
    private DataInputBuffer DataInputBuffer = new DataInputBuffer();
    private DataOutputBuffer DataOutputBuffer = new DataOutputBuffer();

    @Before
    public void setUp() throws Exception {
        // For the test data
        Path path = new Path("input");
        String deviceId = "d1";
        int numOfRowGroupMetaDate = 1;
        List<RowGroupMetaData> rowGroupMetaDataList = new ArrayList<>();
        rowGroupMetaDataList.add(new RowGroupMetaData("d1", 1, 10, new ArrayList<TimeSeriesChunkMetaData>(), "Int"));
        rowGroupMetaDataList.add(new RowGroupMetaData("d1", 2, 20, new ArrayList<TimeSeriesChunkMetaData>(), "Int"));
        rowGroupMetaDataList.add(new RowGroupMetaData("d2", 3, 30, new ArrayList<TimeSeriesChunkMetaData>(), "Float"));
        long start = 5;
        long length = 100;
        String[] hosts = {"192.168.1.1", "192.168.1.0", "localhost"};

        wInputSplit = new TSFInputSplit(path, rowGroupMetaDataList, start, length, hosts);
        rInputSplit = new TSFInputSplit();
    }

    @Test
    public void testInputSplitWriteAndRead() {
        try {
            // call the write method to serialize the object
            wInputSplit.write(DataOutputBuffer);
            DataOutputBuffer.flush();
            DataInputBuffer.reset(DataOutputBuffer.getData(), DataOutputBuffer.getLength());
            rInputSplit.readFields(DataInputBuffer);
            DataInputBuffer.close();
            DataOutputBuffer.close();
            // assert
            assertEquals(wInputSplit.getPath(), rInputSplit.getPath());
            assertEquals(wInputSplit.getNumOfDeviceRowGroup(), rInputSplit.getNumOfDeviceRowGroup());
            //assertEquals(wInputSplit.getDeviceRowGroupMetaDataList(), rInputSplit.getDeviceRowGroupMetaDataList());
            assertEquals(wInputSplit.getStart(), rInputSplit.getStart());
            try {
                assertEquals(wInputSplit.getLength(), rInputSplit.getLength());
                assertArrayEquals(wInputSplit.getLocations(), rInputSplit.getLocations());
            } catch (InterruptedException e) {
                e.printStackTrace();
                fail(e.getMessage());
            }
        } catch (IOException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

}
