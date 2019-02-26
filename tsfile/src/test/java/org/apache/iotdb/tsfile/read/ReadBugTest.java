package org.apache.iotdb.tsfile.read;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.encoding.encoder.Encoder;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetaData;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadataIndex;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.RecordUtils;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.schema.FileSchema;
import org.apache.iotdb.tsfile.write.schema.SchemaBuilder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ReadBugTest {
  public static final String DELTA_OBJECT_UID = "delta-3312";
  private String outputDataFile;
  private String inputDataFile;
  private static ReadOnlyTsFile roTsFile = null;

  private static int rowCount;
  private static int chunkGroupSize;
  private static int pageSize;
  public final long START_TIMESTAMP = 1480562618000L;

  @Before
  public void setUp() throws WriteProcessException, IOException, InterruptedException {
    outputDataFile = "src/test/resources/testTsFile.tsfile";
    TSFileDescriptor.getInstance().getConfig().timeSeriesEncoder = "TS_2DIFF";
    inputDataFile = "src/test/resources/perTestInputData";

    rowCount = 1000;
    chunkGroupSize = 16 * 1024 * 1024;
    pageSize = 10000;
    generateSampleInputDataFile();
    writeToFile();

    TsFileSequenceReader reader = new TsFileSequenceReader(outputDataFile);
    roTsFile = new ReadOnlyTsFile(reader);
  }

  private void generateSampleInputDataFile() throws IOException {
    File file = new File(inputDataFile);
    if (file.exists()) {
      file.delete();
    }
    file.getParentFile().mkdirs();
    FileWriter fw = new FileWriter(file);

    long startTime = START_TIMESTAMP;
    for (int i = 0; i < rowCount; i++) {
      // write d1
      String d1 = "d1," + (startTime + i);
      if (i % 8 == 0) {
        d1 += ",s4," + "dog" + i;
      }
      fw.write(d1 + "\r\n");
    }
    fw.close();
  }

  private void writeToFile() throws IOException, WriteProcessException {
    File file = new File(outputDataFile);
    if (file.exists()) {
      file.delete();
    }

    FileSchema schema = generateTestSchema();

    TSFileDescriptor.getInstance().getConfig().groupSizeInByte = chunkGroupSize;
    TSFileDescriptor.getInstance().getConfig().maxNumberOfPointsInPage = pageSize;
    TsFileWriter innerWriter = new TsFileWriter(file, schema, TSFileDescriptor.getInstance().getConfig());
    Scanner in = getDataFile(inputDataFile);
    assert in != null;
    while (in.hasNextLine()) {
      String str = in.nextLine();
      TSRecord record = RecordUtils.parseSimpleTupleRecord(str, schema);
      innerWriter.write(record);
    }
    innerWriter.close();
    in.close();
  }

  private Scanner getDataFile(String path) {
    File file = new File(path);
    try {
      Scanner in = new Scanner(file);
      return in;
    } catch (FileNotFoundException e) {
      e.printStackTrace();
      return null;
    }
  }

  private FileSchema generateTestSchema() {
    SchemaBuilder schemaBuilder = new SchemaBuilder();
//    schemaBuilder.addSeries("s1", TSDataType.INT32, TSEncoding.RLE);
//    schemaBuilder.addSeries("s2", TSDataType.INT64, TSEncoding.PLAIN);
//    schemaBuilder.addSeries("s3", TSDataType.INT64, TSEncoding.TS_2DIFF);
    schemaBuilder.addSeries("s4", TSDataType.TEXT, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED,
        Collections.singletonMap(Encoder.MAX_STRING_LENGTH, "20"));
//    schemaBuilder.addSeries("s5", TSDataType.BOOLEAN, TSEncoding.RLE);
//    schemaBuilder.addSeries("s6", TSDataType.FLOAT, TSEncoding.RLE, CompressionType.SNAPPY,
//        Collections.singletonMap(Encoder.MAX_POINT_NUMBER, "5"));
//    schemaBuilder.addSeries("s7", TSDataType.DOUBLE, TSEncoding.GORILLA);
    return schemaBuilder.build();
  }

  @After
  public void tearDown() throws IOException {
    if (roTsFile != null) {
      roTsFile.close();
    }

    File file = new File(inputDataFile);
    if (file.exists()) {
      file.delete();
    }
    file = new File(outputDataFile);
    if (file.exists()) {
      file.delete();
    }
  }

  @Test
  public void testRead() throws IOException {
    List<Path> pathList = new ArrayList<>();
    pathList.add(new Path("d1.s4"));
    QueryExpression queryExpression = QueryExpression.create(pathList, null);
    QueryDataSet dataSet = roTsFile.query(queryExpression);

    int count = 0;
    while (dataSet.hasNext()) {
      RowRecord r = dataSet.next();
      System.out.println(count);
      System.out.println(r);
      count++;
    }
  }

  @Test
  public void fixBug() throws IOException {
    TsFileSequenceReader reader = new TsFileSequenceReader(outputDataFile);
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
