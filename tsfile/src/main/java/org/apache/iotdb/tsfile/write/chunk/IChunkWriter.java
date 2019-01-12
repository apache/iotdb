package org.apache.iotdb.tsfile.write.chunk;

import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import java.io.IOException;
import java.math.BigDecimal;

/**
 * IChunkWriter provides a list of writing methods for different value types.
 *
 * @author kangrong
 */
public interface IChunkWriter {

    /**
     * write a time value pair
     */
    void write(long time, int value) throws IOException;

    /**
     * write a time value pair
     */
    void write(long time, long value) throws IOException;

    /**
     * write a time value pair
     */
    void write(long time, boolean value) throws IOException;

    /**
     * write a time value pair
     */
    void write(long time, float value) throws IOException;

    /**
     * write a time value pair
     */
    void write(long time, double value) throws IOException;

    /**
     * write a time value pair
     */
    void write(long time, BigDecimal value) throws IOException;

    /**
     * write a time value pair
     */
    void write(long time, Binary value) throws IOException;

    /**
     * flush data to TsFileIOWriter
     */
    void writeToFileWriter(TsFileIOWriter tsfileWriter) throws IOException;

    /**
     * estimate memory used size of this series
     */
    long estimateMaxSeriesMemSize();

    /**
     * return the serialized size of the chunk header + all pages (not include the un-sealed page).
     * Notice, call this method before calling writeToFileWriter(), otherwise the page buffer in memory will be cleared.
     */
    long getCurrentChunkSize();

    /**
     * seal the current page which may has not enough data points in force.
     *
     */
    void sealCurrentPage();

    int getNumOfPages();
}
