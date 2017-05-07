package cn.edu.thu.tsfiledb.engine.overflow.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfiledb.engine.overflow.thrift.OFFileMetadata;



/**
 * 
 * ConverterUtils is a utility class. It provide conversion between tsfile and thrift metadata
 * class. It also provides function that read/write page header from/to stream
 * 
 * @author XuYi xuyi556677@163.com
 *
 */
public class ReadWriteThriftFormatUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(ReadWriteThriftFormatUtils.class);

  private static void write(TBase<?, ?> tbase, OutputStream to) throws IOException {
    try {
      tbase.write(protocol(to));
    } catch (TException e) {
      LOGGER.error("tsfile-file Utils: can not write {}", tbase, e);
      throw new IOException(e);
    }
  }

  private static <T extends TBase<?, ?>> T read(InputStream from, T tbase) throws IOException {
    try {
      tbase.read(protocol(from));
      return tbase;
    } catch (TException e) {
      LOGGER.error("tsfile-file Utils: can not read {}", tbase, e);
      throw new IOException(e);
    }
  }
  
  /**
   * read overflow file metadata(thrift format) from stream
   * 
   * @param from
   * @throws IOException
   */
  public static OFFileMetadata readOFFileMetaData(InputStream from) throws IOException {
      return ReadWriteThriftFormatUtils.read(from, new OFFileMetadata());
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
      write(ofFileMetadata, to);
  }

  private static TProtocol protocol(OutputStream to) {
    return new TCompactProtocol((new TIOStreamTransport(to)));
  }

  private static TProtocol protocol(InputStream from) {
    return new TCompactProtocol((new TIOStreamTransport(from)));
  }

}
