package cn.edu.tsinghua.tsfile.compress;

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.xerial.snappy.Snappy;

/**
 * 
 * @author kangrong
 *
 */
public class SnappyTest {

    @Before
    public void setUp() throws Exception {}

    @After
    public void tearDown() throws Exception {}

    @Test
    public void test() throws UnsupportedEncodingException, IOException {
        String input =
                "Hello snappy-java! Snappy-java is a JNI-based wrapper of "
                        + "Snappy, a fast compresser/decompresser.";
        byte[] compressed = Snappy.compress(input.getBytes("UTF-8"));
        byte[] uncompressed = Snappy.uncompress(compressed);

        String result = new String(uncompressed, "UTF-8");
        assertEquals(input, result);
    }

}
