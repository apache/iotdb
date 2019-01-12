package org.apache.iotdb.db.engine.overflow.utils;

import org.apache.iotdb.db.engine.overflow.metadata.OFFileMetadata;
import org.apache.iotdb.db.engine.overflow.metadata.OFFileMetadata;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;


/**
 * ConverterUtils is a utility class. It provide conversion between
 * tsfile and thrift overflow metadata class
 */
public class OverflowReadWriteThriftFormatUtils {

    /**
     * read overflow file metadata(thrift format) from stream
     *
     * @param from
     * @throws IOException
     */
    public static OFFileMetadata readOFFileMetaData(InputStream from) throws IOException {
        return OFFileMetadata.deserializeFrom(from);
    }

    /**
     * write overflow metadata(thrift format) to stream
     *
     * @param ofFileMetadata
     * @param to
     * @throws IOException
     */
    public static void writeOFFileMetaData(OFFileMetadata ofFileMetadata, OutputStream to)
            throws IOException {
        ofFileMetadata.serializeTo(to);
    }


}
