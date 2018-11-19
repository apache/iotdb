package cn.edu.tsinghua.tsfile.file.metadata.converter;

import cn.edu.tsinghua.tsfile.file.metadata.TsFileMetaData;
import cn.edu.tsinghua.tsfile.format.FileMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * converter for file metadata
 */
public class TsFileMetaDataConverter {
    private static final Logger LOGGER = LoggerFactory.getLogger(TsFileMetaDataConverter.class);

    /**
     * convert tsfile format file matadata to thrift format file matadata
     *
     * @param fileMetadataInTsFile file metadata in tsfile format
     * @return file metadata in thrift format
     */
    public FileMetaData toThriftFileMetadata(TsFileMetaData fileMetadataInTsFile) {
        try {
            return fileMetadataInTsFile.convertToThrift();
        } catch (Exception e) {
            LOGGER.error("TsFileMetaDataConverter: failed to convert metadata from TsFile to thrift, content is {}",
                    fileMetadataInTsFile, e);
        }
        return null;
    }

    /**
     * convert thrift format file matadata to tsfile format file matadata
     *
     * @param fileMetaDataInThrift file metadata in thrift format
     * @return file metadata in tsfile format
     */
    public TsFileMetaData toTsFileMetadata(FileMetaData fileMetaDataInThrift) {
        TsFileMetaData fileMetaDataInTSFile = new TsFileMetaData();
        try {
            fileMetaDataInTSFile.convertToTSF(fileMetaDataInThrift);
        } catch (Exception e) {
            LOGGER.error("TsFileMetaDataConverter: failed to convert metadata from thrift to TSFile, content is {}",
                    fileMetaDataInThrift, e);
        }
        return fileMetaDataInTSFile;
    }

    public int calculatePageHeaderSize(int digestSize) {
        //PageHeader: PageType--4, uncompressedSize--4,compressedSize--4
        //DatapageHeader: numValues--4, numNulls--4, numRows--4, Encoding--4, isCompressed--1, maxTimestamp--8, minTimestamp--8
        //Digest: max ByteBuffer, min ByteBuffer
        // * 2 to caculate max object size in memory

        return 2 * (45 + digestSize);
    }
}
