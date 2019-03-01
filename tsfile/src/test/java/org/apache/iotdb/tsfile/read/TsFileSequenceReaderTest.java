package org.apache.iotdb.tsfile.read;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetaData;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadataIndex;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetaData;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.TsFileGeneratorForTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TsFileSequenceReaderTest {

  private static final String FILE_PATH = TsFileGeneratorForTest.outputDataFile;
  private TsFileSequenceReader fileReader;
  private int rowCount = 1000;
  private ReadOnlyTsFile tsFile;

  @Before
  public void before() throws InterruptedException, WriteProcessException, IOException {
    TsFileGeneratorForTest.generateFile(rowCount, 16 * 1024 * 1024, 10000);
    fileReader = new TsFileSequenceReader(FILE_PATH);
    tsFile = new ReadOnlyTsFile(fileReader);
  }

  @After
  public void after() throws IOException {
    tsFile.close();
    TsFileGeneratorForTest.after();
  }

  @Test
  public void testReadTsFileSequently() throws IOException {
    TsFileSequenceReader reader = new TsFileSequenceReader(FILE_PATH);
    TsFileMetaData metaData = reader.readFileMetadata();
    List<Pair<Long, Long>> offsetList = new ArrayList<>();
    long startOffset = reader.position();
    byte marker;
    while ((marker = reader.readMarker()) != MetaMarker.SEPARATOR) {
      switch (marker) {
        case MetaMarker.CHUNK_HEADER:
          ChunkHeader header = reader.readChunkHeader();
          for (int j = 0; j < header.getNumOfPages(); j++) {
            PageHeader pageHeader = reader.readPageHeader(header.getDataType());
            reader.readPage(pageHeader, header.getCompressionType());
          }
          break;
        case MetaMarker.CHUNK_GROUP_FOOTER:
          reader.readChunkGroupFooter();
          long endOffset = reader.position();
          offsetList.add(new Pair<>(startOffset, endOffset));
          startOffset = endOffset;
          break;
        default:
          MetaMarker.handleUnexpectedMarker(marker);
      }
    }
    int offsetListIndex = 0;
    List<TsDeviceMetadataIndex> deviceMetadataIndexList = metaData.getDeviceMap().values().stream()
        .sorted((x, y) -> (int) (x.getOffset() - y.getOffset())).collect(Collectors.toList());
    for (TsDeviceMetadataIndex index : deviceMetadataIndexList) {
      TsDeviceMetadata deviceMetadata = reader.readTsDeviceMetaData(index);
      List<ChunkGroupMetaData> chunkGroupMetaDataList = deviceMetadata.getChunkGroupMetaDataList();
      for (ChunkGroupMetaData chunkGroupMetaData : chunkGroupMetaDataList) {
        Pair<Long, Long> pair = offsetList.get(offsetListIndex++);
        Assert.assertEquals(chunkGroupMetaData.getStartOffsetOfChunkGroup(), (long) pair.left);
        Assert.assertEquals(chunkGroupMetaData.getEndOffsetOfChunkGroup(), (long) pair.right);
      }
    }
    reader.close();
  }
}