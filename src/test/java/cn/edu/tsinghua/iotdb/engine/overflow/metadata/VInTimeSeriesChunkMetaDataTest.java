package cn.edu.tsinghua.iotdb.engine.overflow.metadata;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.tsinghua.tsfile.common.utils.TsRandomAccessFileWriter;
import cn.edu.tsinghua.tsfile.file.metadata.TsDigest;
import cn.edu.tsinghua.tsfile.file.metadata.VInTimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.utils.ReadWriteThriftFormatUtils;
import cn.edu.tsinghua.tsfile.format.DataType;
import cn.edu.tsinghua.tsfile.format.Digest;
import cn.edu.tsinghua.tsfile.format.ValueInTimeSeriesChunkMetaData;

public class VInTimeSeriesChunkMetaDataTest {
  private VInTimeSeriesChunkMetaData metaData;
  public static final int MAX_ERROR = 1232;
  public static final String maxString = "3244324";
  public static final String minString = "fddsfsfgd";
  final String PATH = "target/outputV.ksn";
  @Before
  public void setUp() throws Exception {
    metaData = new VInTimeSeriesChunkMetaData();
  }

  @After
  public void tearDown() throws Exception {
    File file = new File(PATH);
    if (file.exists())
      file.delete();
  }

  @Test
  public void testWriteIntoFile() throws IOException {
    VInTimeSeriesChunkMetaData metaData = TestHelper.createSimpleV2InTSF(TSDataType.TEXT, new TsDigest());
    
    File file = new File(PATH);
    if (file.exists())
      file.delete();
    FileOutputStream fos = new FileOutputStream(file);
    TsRandomAccessFileWriter out = new TsRandomAccessFileWriter(file, "rw");
    ReadWriteThriftFormatUtils.write(metaData.convertToThrift(), out.getOutputStream());

    out.close();
    fos.close();

    FileInputStream fis = new FileInputStream(new File(PATH));
    Utils.isVSeriesChunkMetadataEqual(metaData, metaData.convertToThrift());
    Utils.isVSeriesChunkMetadataEqual(metaData,
    		ReadWriteThriftFormatUtils.read(fis, new ValueInTimeSeriesChunkMetaData()));
  }


  @Test
  public void testConvertToThrift() throws UnsupportedEncodingException {
    for (TSDataType dataType : TSDataType.values()) {
      VInTimeSeriesChunkMetaData metaData = new VInTimeSeriesChunkMetaData(dataType);
      Utils.isVSeriesChunkMetadataEqual(metaData, metaData.convertToThrift());

      metaData.setMaxError(3123);
      Utils.isVSeriesChunkMetadataEqual(metaData, metaData.convertToThrift());
      metaData.setMaxError(-11);
      Utils.isVSeriesChunkMetadataEqual(metaData, metaData.convertToThrift());

      TsDigest digest = new TsDigest();
      metaData.setDigest(digest);
      Utils.isVSeriesChunkMetadataEqual(metaData, metaData.convertToThrift());

      metaData.setDigest(TestHelper.createSimpleTsDigest());
      Utils.isVSeriesChunkMetadataEqual(metaData, metaData.convertToThrift());
    }
  }

  @Test
  public void testConvertToTSF() throws UnsupportedEncodingException {
    for (DataType dataType : DataType.values()) {
      ValueInTimeSeriesChunkMetaData valueInTimeSeriesChunkMetaData =
          new ValueInTimeSeriesChunkMetaData(dataType);
      metaData.convertToTSF(valueInTimeSeriesChunkMetaData);
      Utils.isVSeriesChunkMetadataEqual(metaData, valueInTimeSeriesChunkMetaData);

      valueInTimeSeriesChunkMetaData.setMax_error(3123);
      metaData.convertToTSF(valueInTimeSeriesChunkMetaData);
      Utils.isVSeriesChunkMetadataEqual(metaData, valueInTimeSeriesChunkMetaData);

      valueInTimeSeriesChunkMetaData.setMax_error(-231);
      metaData.convertToTSF(valueInTimeSeriesChunkMetaData);
      Utils.isVSeriesChunkMetadataEqual(metaData, valueInTimeSeriesChunkMetaData);

      valueInTimeSeriesChunkMetaData.setDigest(new Digest());
      metaData.convertToTSF(valueInTimeSeriesChunkMetaData);
      Utils.isVSeriesChunkMetadataEqual(metaData, valueInTimeSeriesChunkMetaData);
      
      valueInTimeSeriesChunkMetaData.setDigest(TestHelper.createSimpleDigest());
      metaData.convertToTSF(valueInTimeSeriesChunkMetaData);
      Utils.isVSeriesChunkMetadataEqual(metaData, valueInTimeSeriesChunkMetaData);
    }
  }
}
