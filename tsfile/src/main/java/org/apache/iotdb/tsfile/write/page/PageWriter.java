package org.apache.iotdb.tsfile.write.page;

import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.iotdb.tsfile.encoding.encoder.Encoder;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.encoding.encoder.Encoder;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;

/**
 * This function is used to write time-value into a page. It consists of a time encoder, a
 * value encoder and respective OutputStream.
 *
 * @author kangrong
 */
public class PageWriter {
    // time
    private Encoder timeEncoder;
    private PublicBAOS timeOut;
    // value
    private Encoder valueEncoder;
    private PublicBAOS valueOut;

    public PageWriter() {
        this(null,null);
    }

    public PageWriter(MeasurementSchema measurementSchema){
        this(measurementSchema.getTimeEncoder(),measurementSchema.getValueEncoder());
    }

    public PageWriter(Encoder timeEncoder,Encoder valueEncoder){
        this.timeOut = new PublicBAOS();
        this.valueOut = new PublicBAOS();
        this.timeEncoder = timeEncoder;
        this.valueEncoder = valueEncoder;
    }

    /**
     * write a time value pair into encoder
     */
    public void write(long time, boolean value) throws IOException {
        timeEncoder.encode(time, timeOut);
        valueEncoder.encode(value, valueOut);
    }

    /**
     * write a time value pair into encoder
     */
    public void write(long time, short value) throws IOException {
        timeEncoder.encode(time, timeOut);
        valueEncoder.encode(value, valueOut);
    }

    /**
     * write a time value pair into encoder
     */
    public void write(long time, int value) throws IOException {
        timeEncoder.encode(time, timeOut);
        valueEncoder.encode(value, valueOut);
    }

    /**
     * write a time value pair into encoder
     */
    public void write(long time, long value) throws IOException {
        timeEncoder.encode(time, timeOut);
        valueEncoder.encode(value, valueOut);
    }

    /**
     * write a time value pair into encoder
     */
    public void write(long time, float value) throws IOException {
        timeEncoder.encode(time, timeOut);
        valueEncoder.encode(value, valueOut);
    }

    /**
     * write a time value pair into encoder
     */
    public void write(long time, double value) throws IOException {
        timeEncoder.encode(time, timeOut);
        valueEncoder.encode(value, valueOut);
    }

    /**
     * write a time value pair into encoder
     */
    public void write(long time, BigDecimal value) throws IOException {
        timeEncoder.encode(time, timeOut);
        valueEncoder.encode(value, valueOut);
    }

    /**
     * write a time value pair into encoder
     */
    public void write(long time, Binary value) throws IOException {
        timeEncoder.encode(time, timeOut);
        valueEncoder.encode(value, valueOut);
    }

    /**
     * flush all data remained in encoders.
     */
    private void prepareEndWriteOnePage() throws IOException {
        timeEncoder.flush(timeOut);
        valueEncoder.flush(valueOut);
    }


    /**
     * getUncompressedBytes return data what it has been written in form of <code>size of time list, time list, value list</code>
     *
     * @return a new readable ByteBuffer whose position is 0.
     */
    public ByteBuffer getUncompressedBytes() throws IOException {
        prepareEndWriteOnePage();
        ByteBuffer buffer = ByteBuffer.allocate(timeOut.size() + valueOut.size() + 4);
        int length1 = ReadWriteForEncodingUtils.writeUnsignedVarInt(timeOut.size(), buffer);
        buffer.put(timeOut.getBuf(), 0, timeOut.size());
        buffer.put(valueOut.getBuf(), 0, valueOut.size());
        buffer.flip();
        return buffer;
    }


    /**
     * calculate max possible memory size it occupies, including time outputStream and value outputStream,
     * because size outputStream is never used until flushing.
     *
     * @return allocated size in time, value and outputStream
     */
    public long estimateMaxMemSize() {
        return timeOut.size() + valueOut.size() + timeEncoder.getMaxByteSize() + valueEncoder.getMaxByteSize();
    }

    /**
     * reset data in ByteArrayOutputStream
     */
    public void reset() {
        timeOut.reset();
        valueOut.reset();
    }

    public void setTimeEncoder(Encoder encoder) {
        this.timeEncoder = encoder;
    }

    public void setValueEncoder(Encoder encoder) {
        this.valueEncoder = encoder;
    }

}
