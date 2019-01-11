package cn.edu.tsinghua.tsfile.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

/**
 * A subclass extending <code>ByteArrayOutputStream</code>. It's used to return the byte array directly.
 * Note that the size of byte array is large than actual size of valid contents, thus it's used cooperating
 * with <code>size()</code> or <code>capacity = size</code>
 */
public class PublicBAOS extends ByteArrayOutputStream {

    public PublicBAOS(int size) {
        super(size);
    }

    public PublicBAOS() {
        super();
    }

    /**
     * get current all bytes data
     *
     * @return all bytes data
     */
    public byte[] getBuf() {

        return this.buf;
    }

    /**
     * Construct one {@link ByteArrayInputStream} from the buff data
     *
     * @return one {@link ByteArrayInputStream} have all buff data
     */
    public ByteArrayInputStream transformToInputStream() {
        return new ByteArrayInputStream(this.buf, 0, size());
    }


}
