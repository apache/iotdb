package org.apache.iotdb.tsfile.file.metadata.statistics;

import org.apache.iotdb.tsfile.exception.write.UnknownColumnTypeException;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;

/**
 * This class is used for recording statistic information of each measurement in a delta file.While
 * writing processing, the processor records the digest information. Statistics includes maximum,
 * minimum and null value count up to version 0.0.1.<br>
 * Each data type extends this Statistic as super class.<br>
 *
 * @param <T> data type for Statistics
 * @author kangrong
 * @since 0.0.1
 */
public abstract class Statistics<T> {
    private static final Logger LOG = LoggerFactory.getLogger(Statistics.class);
    // isEmpty being false means this statistic has been initialized and the max
    // and min is not null;
    protected boolean isEmpty = true;

    /**
     * static method providing statistic instance for respective data type.
     *
     * @param type - data type
     * @return Statistics
     */
    public static Statistics<?> getStatsByType(TSDataType type) {
        switch (type) {
            case INT32:
                return new IntegerStatistics();
            case INT64:
                return new LongStatistics();
            case TEXT:
                return new BinaryStatistics();
            case BOOLEAN:
                return new BooleanStatistics();
            case DOUBLE:
                return new DoubleStatistics();
            case FLOAT:
                return new FloatStatistics();
            default:
                throw new UnknownColumnTypeException(type.toString());
        }
    }

    abstract public void setMinMaxFromBytes(byte[] minBytes, byte[] maxBytes);

    abstract public T getMin();

    abstract public T getMax();
    
    abstract public T getFirst();
    
    abstract public double getSum();
    
    abstract public T getLast();

    abstract public byte[] getMaxBytes();

    abstract public byte[] getMinBytes();

    abstract public byte[] getFirstBytes();

    abstract public byte[] getSumBytes();

    abstract public byte[] getLastBytes();

    abstract public ByteBuffer getMaxBytebuffer();

    abstract public ByteBuffer getMinBytebuffer();

    abstract public ByteBuffer getFirstBytebuffer();

    abstract public ByteBuffer getSumBytebuffer();

    abstract public ByteBuffer getLastBytebuffer();



    /**
     * merge parameter to this statistic. Including
     *
     * @param stats input statistics
     * @throws StatisticsClassException cannot merge statistics
     */
    public void mergeStatistics(Statistics<?> stats) throws StatisticsClassException {
        if (stats == null) {
            LOG.warn("tsfile-file parameter stats is null");
            return;
        }
        if (this.getClass() == stats.getClass()) {
            if (!stats.isEmpty) {
            	mergeStatisticsValue(stats);
                isEmpty = false;
            }
        } else {
            LOG.warn("tsfile-file Statistics classes mismatched,no merge: "
                    + this.getClass().toString() + " vs. " + stats.getClass().toString());

            throw new StatisticsClassException(this.getClass(), stats.getClass());
        }
    }

    abstract protected void mergeStatisticsValue(Statistics<?> stats);

    public boolean isEmpty() {
        return isEmpty;
    }

    public void updateStats(boolean value) {
        throw new UnsupportedOperationException();
    }

    public void updateStats(int value) {
        throw new UnsupportedOperationException();
    }

    public void updateStats(long value) {
        throw new UnsupportedOperationException();
    }

    /**
     * This method with two parameters is only used by {@code overflow} which
     * updates/inserts/deletes timestamp.
     *
     * @param min min timestamp
     * @param max max timestamp
     */
    public void updateStats(long min, long max){
    	throw new UnsupportedOperationException();
    }
        

    public void updateStats(float value) {
        throw new UnsupportedOperationException();
    }

    public void updateStats(double value) {
        throw new UnsupportedOperationException();
    }

    public void updateStats(BigDecimal value) {
        throw new UnsupportedOperationException();
    }

    public void updateStats(Binary value) {
        throw new UnsupportedOperationException();
    }

    public void reset() {
    }


    /**
     *
     * @return the size of one field of this class.<br>
     *     int, float - 4<br>
     *     double, long, bigDecimal - 8 <br>
     *     boolean - 1 <br>
     *     No - 0 <br>
     *     binary - -1 which means uncertainty
     *     </>
     */
    abstract public int sizeOfDatum();

    /**
     * read data from the inputStream.
     * @param inputStream
     * @throws IOException
     */
    abstract void fill(InputStream inputStream) throws IOException;

    abstract void fill(ByteBuffer byteBuffer) throws IOException;


    public int getSerializedSize() {
        if(sizeOfDatum()==0){
            return 0;
        }
        else if(sizeOfDatum()!=-1) {
            return sizeOfDatum() * 4 + 8;
        }else{
            return 4*Integer.BYTES + getMaxBytes().length + getMinBytes().length +getFirstBytes().length+getLastBytes().length +getSumBytes().length;
        }
    }

    public int serialize(OutputStream outputStream) throws IOException {
        int length=0;
        if(sizeOfDatum()==0){
            return 0;
        }
        else if(sizeOfDatum()!=-1) {
            length = sizeOfDatum() * 4 + 8;
            outputStream.write(getMinBytes());
            outputStream.write(getMaxBytes());
            outputStream.write(getFirstBytes());
            outputStream.write(getLastBytes());
            outputStream.write(getSumBytes());
        }else{
            byte[] tmp=getMinBytes();
            length+=tmp.length;
            length+=ReadWriteIOUtils.write(tmp.length,outputStream);
            outputStream.write(tmp);
            tmp=getMaxBytes();
            length+=tmp.length;
            length+=ReadWriteIOUtils.write(tmp.length,outputStream);
            outputStream.write(tmp);
            tmp=getFirstBytes();
            length+=tmp.length;
            length+=ReadWriteIOUtils.write(tmp.length,outputStream);
            outputStream.write(tmp);
            tmp=getLastBytes();
            length+=tmp.length;
            length+=ReadWriteIOUtils.write(tmp.length,outputStream);
            outputStream.write(tmp);
            outputStream.write(getSumBytes());
            length+=8;
        }
        return length;
    }

    public static Statistics deserialize(InputStream inputStream, TSDataType dataType) throws IOException {
        Statistics statistics=null;
        switch (dataType) {
            case INT32:
                 statistics = new IntegerStatistics();
                 break;
            case INT64:
                statistics = new LongStatistics();
                break;
            case TEXT:
                statistics = new BinaryStatistics();
                break;
            case BOOLEAN:
                statistics = new BooleanStatistics();
                break;
            case DOUBLE:
                statistics = new DoubleStatistics();
                break;
            case FLOAT:
                statistics = new FloatStatistics();
                break;
            default:
                throw new UnknownColumnTypeException(dataType.toString());
        }
        statistics.fill(inputStream);
        return statistics;
    }

    public static Statistics deserialize(ByteBuffer buffer, TSDataType dataType) throws IOException {
        Statistics statistics=null;
        switch (dataType) {
            case INT32:
                statistics = new IntegerStatistics();
                break;
            case INT64:
                statistics = new LongStatistics();
                break;
            case TEXT:
                statistics = new BinaryStatistics();
                break;
            case BOOLEAN:
                statistics = new BooleanStatistics();
                break;
            case DOUBLE:
                statistics = new DoubleStatistics();
                break;
            case FLOAT:
                statistics = new FloatStatistics();
                break;
            default:
                throw new UnknownColumnTypeException(dataType.toString());
        }
        statistics.fill(buffer);
        return statistics;
    }


}
