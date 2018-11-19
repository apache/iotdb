package cn.edu.tsinghua.tsfile.common.utils;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

/***
 * This class is designed for maintaining several <code>ByteArrayOutputStream</code> and provides
 * functions including writeAllTo, size and reset.
 *
 * @author kangrong
 *
 */
public class ListByteArrayOutputStream {
    private List<PublicBAOS> list;
    private int totalSize = 0;

    public ListByteArrayOutputStream(PublicBAOS... param) {
        list = new ArrayList<>();
        for (PublicBAOS out : param) {
            list.add(out);
            totalSize += out.size();
        }
    }

    /**
     * Constructs ListByteArrayOutputStream using ByteArrayOutputStream.
     *
     * @param out the data source for constructing a <code>ListByteArrayOutputStream</code>
     * @return a new <code>ListByteArrayOutputStream</code> containing data in <code>out</code>.
     */
    public static ListByteArrayOutputStream from(PublicBAOS out) {
        return new ListByteArrayOutputStream(out);
    }

    /**
     * Inputs an OutputStream as parameter. Writes the complete contents in <code>list</code> to
     * the specified output stream argument.
     *
     * @param out the output stream to write the data.
     * @throws IOException if an I/O error occurs.
     */
    public void writeAllTo(OutputStream out) throws IOException {
        for (PublicBAOS baos : list)
            baos.writeTo(out);
    }

    /**
     * get the total size of this class
     *
     * @return total size
     */
    public int size() {
        return totalSize;
    }

    /**
     * Creates a new <code>PublicBAOS</code> which specified size is the current
     * total size and write the current contents in <code>list</code> into it.
     *
     * @return the current contents of this class, as a byte array.
     * @throws IOException if an I/O error occurs.
     */
    public byte[] toByteArray() throws IOException {
        PublicBAOS baos = new PublicBAOS(totalSize);
        this.writeAllTo(baos);
        return baos.getBuf();
    }

    /**
     * Appends a <code>ByteArrayOutputStream</code> into this class.
     *
     * @param out a output stream to be appended.
     */
    public void append(PublicBAOS out) {
        list.add(out);
        totalSize += out.size();
    }

    /**
     * Resets the <code>list</code> and <code>totalSize</code> fields.
     */
    public void reset() {
        list.clear();
        totalSize = 0;
    }
}
