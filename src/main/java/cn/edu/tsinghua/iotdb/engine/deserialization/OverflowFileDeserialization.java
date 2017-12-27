package cn.edu.tsinghua.iotdb.engine.deserialization;

import cn.edu.tsinghua.iotdb.engine.overflow.io.OverflowFileIO;
import cn.edu.tsinghua.iotdb.engine.overflow.io.OverflowReadWriter;
import cn.edu.tsinghua.iotdb.engine.overflow.metadata.OFFileMetadata;
import cn.edu.tsinghua.iotdb.engine.overflow.metadata.OFRowGroupListMetadata;
import cn.edu.tsinghua.iotdb.engine.overflow.metadata.OFSeriesListMetadata;
import cn.edu.tsinghua.iotdb.engine.overflow.utils.OverflowReadWriteThriftFormatUtils;
import cn.edu.tsinghua.iotdb.engine.overflow.utils.TSFileMetaDataConverter;
import cn.edu.tsinghua.tsfile.common.utils.BytesUtils;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Map;


/**
 * An Overflow FileDeserialization program.
 */
public class OverflowFileDeserialization {

    private static final Logger LOGGER = LoggerFactory.getLogger(OverflowFileDeserialization.class);

    private static String folderName = "your file name";;
    private String restoreFileName;
    private String mergeFileName;

    public OverflowFileDeserialization() {
    }


    public static void main(String[] args) throws IOException {
        File dirFile = new File(folderName);
        if (!dirFile.exists() || (!dirFile.isDirectory())) {
            LOGGER.error("the given folder name {} is wrong", folderName);
            return;
        }

        OverflowFileDeserialization deserialization = new OverflowFileDeserialization();
        long overflowNumber = 0;
        File[] subFiles = dirFile.listFiles();
        if (subFiles == null || subFiles.length == 0) {
            LOGGER.error("the given folder {} has no overflow folder", folderName);
            return;
        }
        for (File file : subFiles) {
            if (file.isDirectory()) {
                File[] ofFiles = file.listFiles();
                if (ofFiles.length == 2) {
                    for (File f : ofFiles) {
                        if (f.getAbsolutePath().endsWith(".restore")) {
                            long tmpNumber = deserialization.getOverflowRowNumbersForRestoreFile(f.getAbsolutePath());
                            System.out.println(f.getAbsolutePath() + " " + tmpNumber);
                            overflowNumber += tmpNumber;
                        }
                    }
                } else if (ofFiles.length == 3) {
                    for (File f : ofFiles) {
                        if (f.getAbsolutePath().endsWith(".merge")) {
                            long tmpNumber = deserialization.getOverflowRowNumbersForMergeFile(f.getAbsolutePath());
                            System.out.println(f.getAbsolutePath() + " " + tmpNumber);
                            overflowNumber += tmpNumber;
                        }
                    }
                }
            }
        }
        System.out.println("Final result : " + overflowNumber);
    }


    /**
     * This method is used to calculate the overflow write size using the overflow.merge file.
     *
     * @param ofMergeFileName overflow file name which ends with '.merge'
     * @return the number of overflow write rows
     * @throws IOException file read error
     */
    private long getOverflowRowNumbersForMergeFile(String ofMergeFileName) throws IOException {
        OverflowReadWriter overflowReadWriter = new OverflowReadWriter(ofMergeFileName);
        OverflowFileIO io = new OverflowFileIO(overflowReadWriter, "", overflowReadWriter.length());
        Map<String, Map<String, List<TimeSeriesChunkMetaData>>> ans = io.getSeriesListMap();

        long rowNumber = 0;
        for (Map.Entry<String, Map<String, List<TimeSeriesChunkMetaData>>> entry : ans.entrySet()) {
            Map<String, List<TimeSeriesChunkMetaData>> measurementMap = entry.getValue();
            for (Map.Entry<String, List<TimeSeriesChunkMetaData>> entry1 : measurementMap.entrySet()) {
                for (TimeSeriesChunkMetaData timeSeriesChunkMetaData : entry1.getValue())
                    rowNumber += timeSeriesChunkMetaData.getNumRows();
            }
        }

        return rowNumber;
    }

    /**
     * This method is used to calculate the overflow write size using the overflow.restore file.
     * <p>
     * <p> Note that: this estimation may be not accurate.
     *
     * @return the number of overflow write rows
     * @throws IOException file read error
     */
    private long getOverflowRowNumbersForRestoreFile(String ofRestoreFileName) throws IOException {
        OverflowStoreStruct struct = getLastPos(ofRestoreFileName);

        long rowNumber = 0;
        for (OFRowGroupListMetadata rowGroupMetadata : struct.ofFileMetadata.getRowGroupLists()) {
            for (OFSeriesListMetadata oFSeriesListMetadata : rowGroupMetadata.getSeriesLists()) {
                for (TimeSeriesChunkMetaData timeSeriesChunkMetaData : oFSeriesListMetadata.getMetaDatas())
                    rowNumber += timeSeriesChunkMetaData.getNumRows();
            }
        }

        return rowNumber;
    }

    private OverflowStoreStruct getLastPos(String overflowRestoreFilePath) throws FileNotFoundException {

        File overflowRestoreFile = new File(overflowRestoreFilePath);
        if (!overflowRestoreFile.exists()) {
            LOGGER.error("given restore file {} does not exist", overflowRestoreFile);
        }
        byte[] buff = new byte[8];
        FileInputStream fileInputStream = fileInputStream = new FileInputStream(overflowRestoreFile);
        int off = 0;
        int len = buff.length - off;
        cn.edu.tsinghua.iotdb.engine.overflow.thrift.OFFileMetadata thriftfileMetadata = null;
        try {
            do {
                int num = fileInputStream.read(buff, off, len);
                off = off + num;
                len = len - num;
            } while (len > 0);
            long lastOverflowFilePosition = BytesUtils.bytesToLong(buff);

            if (lastOverflowFilePosition != -1) {
                return new OverflowStoreStruct(lastOverflowFilePosition, -1, null);
            }

            off = 0;
            len = buff.length - off;
            do {
                int num = fileInputStream.read(buff, off, len);
                off = off + num;
                len = len - num;
            } while (len > 0);

            long lastOverflowRowGroupPosition = BytesUtils.bytesToLong(buff);
            thriftfileMetadata = OverflowReadWriteThriftFormatUtils.readOFFileMetaData(fileInputStream);
            TSFileMetaDataConverter metadataConverter = new TSFileMetaDataConverter();
            OFFileMetadata ofFileMetadata = metadataConverter.toOFFileMetadata(thriftfileMetadata);
            return new OverflowStoreStruct(lastOverflowFilePosition, lastOverflowRowGroupPosition, ofFileMetadata);
        } catch (IOException e) {
            LOGGER.error(
                    "Read the data: lastOverflowFilePosition, lastOverflowRowGroupPosition, offilemetadata meets error : "
            + e.getMessage());
        } finally {
            if (fileInputStream != null) {
                try {
                    fileInputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        return null;

    }

    private class OverflowStoreStruct {
        public final long lastOverflowFilePosition;
        public final long lastOverflowRowGroupPosition;
        public final OFFileMetadata ofFileMetadata;

        public OverflowStoreStruct(long lastOverflowFilePosition, long lastOverflowRowGroupPosition,
                                   OFFileMetadata ofFileMetadata) {
            this.lastOverflowFilePosition = lastOverflowFilePosition;
            this.lastOverflowRowGroupPosition = lastOverflowRowGroupPosition;
            this.ofFileMetadata = ofFileMetadata;
        }
    }
}
