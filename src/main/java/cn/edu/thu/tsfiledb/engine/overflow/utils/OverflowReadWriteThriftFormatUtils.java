package cn.edu.thu.tsfiledb.engine.overflow.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import cn.edu.thu.tsfile.file.utils.ReadWriteThriftFormatUtils;
import cn.edu.thu.tsfiledb.engine.overflow.thrift.OFFileMetadata;


/**
 * 
 * ConverterUtils is a utility class. It provide conversion between 
 * tsfile and thrift overflow metadata class
 * 
 * @author XuYi xuyi556677@163.com
 *
 */
public class OverflowReadWriteThriftFormatUtils {

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
	  ReadWriteThriftFormatUtils.write(ofFileMetadata, to);
  }


}
