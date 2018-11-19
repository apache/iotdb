package cn.edu.tsinghua.tsfile.file.metadata;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.format.DataType;
import cn.edu.tsinghua.tsfile.format.TimeInTimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.common.utils.TsRandomAccessFileWriter;
import cn.edu.tsinghua.tsfile.file.metadata.utils.TestHelper;
import cn.edu.tsinghua.tsfile.file.metadata.utils.Utils;
import cn.edu.tsinghua.tsfile.file.utils.ReadWriteThriftFormatUtils;
import cn.edu.tsinghua.tsfile.format.FreqType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSFreqType;

public class TInTimeSeriesChunkMetaDataTest {
  private TInTimeSeriesChunkMetaData metaData;
  public static List<Integer> frequencies1;
  public static List<Integer> frequencies2;
  public static final long startTime = 523372036854775806L;;
  public static final long endTime = 523372036854775806L;;
  final String PATH = "target/outputT.ksn";

  @Before
  public void setUp() throws Exception {
    metaData = new TInTimeSeriesChunkMetaData();
    frequencies1 = new ArrayList<Integer>();

    frequencies2 = new ArrayList<Integer>();
    frequencies2.add(132);
    frequencies2.add(432);
    frequencies2.add(35435);
  }

  @After
  public void tearDown() throws Exception {
    File file = new File(PATH);
    if (file.exists())
      file.delete();
  }

  @Test
  public void testWriteIntoFile() throws IOException {
    TInTimeSeriesChunkMetaData metaData = TestHelper.createT2inTSF(TSDataType.TEXT,
        TSFreqType.IRREGULAR_FREQ, frequencies2, startTime, endTime);
    File file = new File(PATH);
    if (file.exists())
      file.delete();
    FileOutputStream fos = new FileOutputStream(file);
    TsRandomAccessFileWriter out = new TsRandomAccessFileWriter(file, "rw");
    ReadWriteThriftFormatUtils.write(metaData.convertToThrift(), out.getOutputStream());

    out.close();
    fos.close();

    FileInputStream fis = new FileInputStream(new File(PATH));
    Utils.isTSeriesChunkMetadataEqual(metaData, metaData.convertToThrift());
    Utils.isTSeriesChunkMetadataEqual(metaData,
    		ReadWriteThriftFormatUtils.read(fis, new TimeInTimeSeriesChunkMetaData()));
  }

  @Test
  public void testConvertToThrift() {
    for (TSDataType dataType : TSDataType.values()) {
      TInTimeSeriesChunkMetaData metaData =
          new TInTimeSeriesChunkMetaData(dataType, startTime, endTime);
      Utils.isTSeriesChunkMetadataEqual(metaData, metaData.convertToThrift());
      for (TSFreqType freqType : TSFreqType.values()) {
        metaData.setFreqType(freqType);
        Utils.isTSeriesChunkMetadataEqual(metaData, metaData.convertToThrift());

        List<Integer> frequencies = new ArrayList<Integer>();
        metaData.setFrequencies(frequencies);
        Utils.isTSeriesChunkMetadataEqual(metaData, metaData.convertToThrift());

        frequencies.add(132);
        frequencies.add(432);
        frequencies.add(35435);
        metaData.setFrequencies(frequencies);
        Utils.isTSeriesChunkMetadataEqual(metaData, metaData.convertToThrift());

        frequencies.clear();
        metaData.setFrequencies(frequencies);
        Utils.isTSeriesChunkMetadataEqual(metaData, metaData.convertToThrift());
      }
    }
  }

  @Test
  public void testConvertToTSF() {
    for (DataType dataType : DataType.values()) {
      TimeInTimeSeriesChunkMetaData timeInTimeSeriesChunkMetaData =
          new TimeInTimeSeriesChunkMetaData(dataType, startTime, endTime);
      metaData.convertToTSF(timeInTimeSeriesChunkMetaData);
      Utils.isTSeriesChunkMetadataEqual(metaData, timeInTimeSeriesChunkMetaData);
      for (FreqType freqType : FreqType.values()) {
        timeInTimeSeriesChunkMetaData.setFreq_type(freqType);

        metaData.convertToTSF(timeInTimeSeriesChunkMetaData);
        Utils.isTSeriesChunkMetadataEqual(metaData, timeInTimeSeriesChunkMetaData);

        metaData.convertToTSF(timeInTimeSeriesChunkMetaData);
        Utils.isTSeriesChunkMetadataEqual(metaData, timeInTimeSeriesChunkMetaData);

        List<Integer> frequencies = new ArrayList<Integer>();
        timeInTimeSeriesChunkMetaData.setFrequencies(frequencies);
        metaData.convertToTSF(timeInTimeSeriesChunkMetaData);
        Utils.isTSeriesChunkMetadataEqual(metaData, timeInTimeSeriesChunkMetaData);

        frequencies.add(132);
        frequencies.add(432);
        frequencies.add(35435);
        timeInTimeSeriesChunkMetaData.setFrequencies(frequencies);
        metaData.convertToTSF(timeInTimeSeriesChunkMetaData);
        Utils.isTSeriesChunkMetadataEqual(metaData, timeInTimeSeriesChunkMetaData);

        frequencies.clear();
        timeInTimeSeriesChunkMetaData.setFrequencies(frequencies);
        metaData.convertToTSF(timeInTimeSeriesChunkMetaData);
        Utils.isTSeriesChunkMetadataEqual(metaData, timeInTimeSeriesChunkMetaData);
      }
    }
  }
}
