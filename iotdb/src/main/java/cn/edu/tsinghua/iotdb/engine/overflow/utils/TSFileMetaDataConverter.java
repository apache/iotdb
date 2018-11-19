package cn.edu.tsinghua.iotdb.engine.overflow.utils;

import cn.edu.tsinghua.iotdb.engine.overflow.metadata.OFFileMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Description converter for file metadata
 * @author XuYi xuyi556677@163.com
 * @date Apr 29, 2016 10:06:10 PM
 */
public class TSFileMetaDataConverter {
  private static final Logger LOGGER = LoggerFactory.getLogger(TSFileMetaDataConverter.class);


  /**
   * @Description convert thrift format overflow file matadata to tsfile format overflow file matadata. For more
   *              information about thrift format overflow file matadata, see
   *              {@code com.corp.delta.tsfile.format.OFFileMetadata} a in tsfile-format
   * @param ofFileMetaData - overflow file metadata in thrift format
   * @return OFFileMetadata - overflow file metadata in tsfile format
   */
  public OFFileMetadata toOFFileMetadata(cn.edu.tsinghua.iotdb.engine.overflow.thrift.OFFileMetadata ofFileMetaDataThrift) {
	  OFFileMetadata ofFileMetadata = new OFFileMetadata();
      try {
    	  ofFileMetadata.convertToTSF(ofFileMetaDataThrift);
      } catch (Exception e) {
          LOGGER.error(
                  "tsfile-file TSFMetaDataConverter: failed to convert from overflow file metadata from thrift to TSFile, content is {}",
                  ofFileMetaDataThrift, e);
      }
	  return ofFileMetadata;
  }
  
  /**
   * @Description convert tsfile format overflow file matadata to thrift format overflow file
   *              matadata. For more information about thrift format file matadata, see
   *              {@code com.corp.delta.tsfile.format.OFFileMetadata} in tsfile-format
   * @param currentVersion - current verison
   * @param ofFileMetadata - overflow file metadata in tsfile format
   * @return org.corp.tsfile.format.OFFileMetaData - overflow file metadata in thrift format
   * @throws
   */
  public cn.edu.tsinghua.iotdb.engine.overflow.thrift.OFFileMetadata toThriftOFFileMetadata(int currentVersion,
                                                                                            OFFileMetadata ofFileMetadata) {
      try {
          return ofFileMetadata.convertToThrift();
      } catch (Exception e) {
          LOGGER.error(
                  "tsfile-file TSFMetaDataConverter: failed to convert overflow file metadata from TSFile to thrift, content is {}",
                  ofFileMetadata, e);
      }
      return null;
  }


}
