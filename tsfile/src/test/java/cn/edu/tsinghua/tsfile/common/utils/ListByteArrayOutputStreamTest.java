package cn.edu.tsinghua.tsfile.common.utils;

import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * This class is used for testing functions of <code>ListByteOutputStream</code>.
 * @author kangrong
 */
public class ListByteArrayOutputStreamTest {
    private byte[] b1 = new byte[]{0, 1, 2};
    private byte[] b2 = new byte[]{3};
    private byte[] b3 = new byte[]{4, 5, 6, 7};
    private byte[] b4 = new byte[]{8, 9, 10};

    private PublicBAOS s1;
    private PublicBAOS s2;
    private PublicBAOS s3;
    private PublicBAOS s4;
    private PublicBAOS total;

    @Before
    public void before() throws IOException {
        s1 = new PublicBAOS();
        s1.write(b1);
        s2 = new PublicBAOS();
        s2.write(b2);
        s3 = new PublicBAOS();
        s3.write(b3);
        s4 = new PublicBAOS();
        s4.write(b4);
        total = new PublicBAOS();
        total.write(b1);
        total.write(b2);
        total.write(b3);
        total.write(b4);
    }

    @Test
    public void testAppend() {
        try {
            ListByteArrayOutputStream listStream = new ListByteArrayOutputStream();
            listStream.append(s1);
            listStream.append(s2);
            listStream.append(s3);
            listStream.append(s4);
            assertEquals(11, listStream.size());
            byte[] ret = listStream.toByteArray();
            for (int i = 0; i < ret.length; i++) {
                assertEquals(i, ret[i]);
            }
        } catch (IOException e) {
            fail();
        }
    }

    @Test
    public void testFrom() {
        try {
            ListByteArrayOutputStream listStream = ListByteArrayOutputStream.from(total);
            assertEquals(11, listStream.size());
            byte[] ret = listStream.toByteArray();
            for (int i = 0; i < ret.length; i++) {
                assertEquals(i, ret[i]);
            }
            listStream.reset();
            assertEquals(0, listStream.size());
        } catch (IOException e) {
            fail();
        }
    }


    @Test
    public void testToArray() {
        try {
            ListByteArrayOutputStream listStream = new ListByteArrayOutputStream(s1, s2, s3, s4);
            assertEquals(11, listStream.size());
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            listStream.writeAllTo(out);
            byte[] ret = out.toByteArray();
            for (int i = 0; i < ret.length; i++) {
                assertEquals(i, ret[i]);
            }
            assertEquals(11, listStream.size());
            listStream.reset();
            assertEquals(0, listStream.size());
        } catch (IOException e) {
            fail();
        }
    }

}
