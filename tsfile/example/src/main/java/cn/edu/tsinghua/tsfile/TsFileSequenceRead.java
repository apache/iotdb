package org.apache.iotdb.tsfile;

import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.footer.ChunkGroupFooter;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetaData;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadataIndex;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.reader.page.PageReader;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;

public class TsFileSequenceRead {

    public static void main(String[] args) throws IOException {
        TsFileSequenceReader reader = new TsFileSequenceReader("test.tsfile");
        System.out.println("file length: " + new File("test.tsfile").length());
        System.out.println("file magic head: " + reader.readHeadMagic());
        System.out.println("file magic tail: " + reader.readTailMagic());
        System.out.println("Level 1 metadata position: " + reader.getFileMetadataPos());
        System.out.println("Level 1 metadata size: " + reader.getFileMetadataPos());
        TsFileMetaData metaData = reader.readFileMetadata();
        // Sequential reading of one ChunkGroup now follows this order:
        // first SeriesChunks (headers and data) in one ChunkGroup, then the ChunkGroupFooter
        // Because we do not know how many chunks a ChunkGroup may have, we should read one byte (the marker) ahead and
        // judge accordingly.
        System.out.println("[Chunk Group]");
        System.out.println("position: " + reader.position());
        byte marker;
        while ((marker = reader.readMarker()) != MetaMarker.Separator) {
            switch (marker) {
                case MetaMarker.ChunkHeader:
                    System.out.println("\t[Chunk]");
                    System.out.println("\tposition: " + reader.position());
                    ChunkHeader header = reader.readChunkHeader();
                    System.out.println("\tMeasurement: " + header.getMeasurementID());
                    Decoder defaultTimeDecoder = Decoder.getDecoderByType(TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().timeSeriesEncoder),
                            TSDataType.INT64);
                    Decoder valueDecoder = Decoder.getDecoderByType(header.getEncodingType(), header.getDataType());
                    for (int j = 0; j < header.getNumOfPages(); j++) {
                        System.out.println("\t\t[Page]\n \t\tPage head position: " + reader.position());
                        PageHeader pageHeader = reader.readPageHeader(header.getDataType());
                        System.out.println("\t\tPage data position: " + reader.position());
                        System.out.println("\t\tpoints in the page: " + pageHeader.getNumOfValues());
                        ByteBuffer pageData = reader.readPage(pageHeader, header.getCompressionType());
                        System.out.println("\t\tUncompressed page data size: " + pageHeader.getUncompressedSize());
                        PageReader reader1 = new PageReader(pageData, header.getDataType(), valueDecoder, defaultTimeDecoder);
                        while (reader1.hasNextBatch()) {
                            BatchData batchData = reader1.nextBatch();
                            while (batchData.hasNext()) {
                                System.out.println("\t\t\ttime, value: " + batchData.currentTime() + ", " + batchData.currentValue());
                                batchData.next();
                            }
                        }
                    }
                    break;
                case MetaMarker.ChunkGroupFooter:
                    System.out.println("Chunk Group Footer position: " + reader.position());
                    ChunkGroupFooter chunkGroupFooter = reader.readChunkGroupFooter();
                    System.out.println("device: " + chunkGroupFooter.getDeviceID());
                    break;
                default:
                    MetaMarker.handleUnexpectedMarker(marker);
            }
        }
        System.out.println("[Metadata]");
        List<TsDeviceMetadataIndex> deviceMetadataIndexList = metaData.getDeviceMap().values().stream().sorted((x, y) -> (int) (x.getOffset() - y.getOffset())).collect(Collectors.toList());
        for (TsDeviceMetadataIndex index : deviceMetadataIndexList) {
            TsDeviceMetadata deviceMetadata = reader.readTsDeviceMetaData(index);
            List<ChunkGroupMetaData> chunkGroupMetaDataList = deviceMetadata.getChunkGroups();
            for (ChunkGroupMetaData chunkGroupMetaData : chunkGroupMetaDataList) {
                System.out.println(String.format("\t[Device]File Offset: %d, Device %s, Number of Chunk Groups %d", index.getOffset(), chunkGroupMetaData.getDeviceID(), chunkGroupMetaDataList.size()));
                for (ChunkMetaData chunkMetadata : chunkGroupMetaData.getChunkMetaDataList()) {
                    System.out.println("\t\tMeasurement:" + chunkMetadata.getMeasurementUID());
                    System.out.println("\t\tFile offset:" + chunkMetadata.getOffsetOfChunkHeader());
                }
            }
        }
        reader.close();
    }
}
