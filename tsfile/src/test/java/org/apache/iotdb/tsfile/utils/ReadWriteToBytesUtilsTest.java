package org.apache.iotdb.tsfile.utils;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class ReadWriteToBytesUtilsTest {

    @Test
    public void testShort() throws IOException {
        for (short i : new short[]{1,2,3,4,5}){
            ByteArrayOutputStream outputstream=new ByteArrayOutputStream();
            ReadWriteIOUtils.write(i, outputstream);
            int size=outputstream.size();
            byte[] bytes=outputstream.toByteArray();
            ByteArrayInputStream inputStream=new ByteArrayInputStream(bytes);
            short k = ReadWriteIOUtils.readShort(inputStream);
            assert  i ==k;
        }
    }
    @Test
    public void testShort2() throws IOException {
        for (short i : new short[]{1,2,3,4,5}){
            ByteBuffer output=ByteBuffer.allocate(2);
            ReadWriteIOUtils.write(i, output);
            output.flip();
            short k = ReadWriteIOUtils.readShort(output);
            assert  i ==k;
        }
    }
    @Test
    public void testShort3() throws IOException {
        for (short i : new short[]{1,2,3,4,5}){
            ByteArrayOutputStream outputstream=new ByteArrayOutputStream();
            ReadWriteIOUtils.write(i, outputstream);
            int size=outputstream.size();
            byte[] bytes=outputstream.toByteArray();
            ByteBuffer buffer= ByteBuffer.wrap(bytes);
            short k = ReadWriteIOUtils.readShort(buffer);
            assert  i ==k;
        }
    }

}
