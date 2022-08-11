/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.tool.core.util;

import org.apache.iotdb.tool.core.model.DsTypeEncodeModel;
import org.apache.iotdb.tool.core.model.EncodeCompressAnalysedModel;
import org.apache.iotdb.tsfile.compress.ICompressor;
import org.apache.iotdb.tsfile.encoding.encoder.*;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class TsFileEncodeCompressAnalysedUtil {
  /** support compression type array */
  private static final CompressionType[] compressTypes =
      new CompressionType[] {
        CompressionType.SNAPPY,
        CompressionType.GZIP,
        CompressionType.LZ4,
        CompressionType.UNCOMPRESSED
      };

  /** compressed rate weight */
  private static final double compressedWeight = 2.5;

  /** compressed sequence weight */
  private static final double compressedSequenceWeight = 2.5;

  /** compressed cost weight */
  private static final double compressedCostWeight = 5;

  /** score rate */
  private static final double zeroRate = 0.8;

  /**
   * generate encode and compress analysed with batchData
   *
   * @param batchData batch data
   * @return EncodeCompressAnalysedModel list
   * @throws IOException throw io exception
   */
  public static List<EncodeCompressAnalysedModel> generateEncodeAndCompressAnalysedWithBatchData(
      BatchData batchData) throws IOException {
    //
    DsTypeEncodeModel encodeModel = generateDsTypeEncodeModel(batchData.getDataType());
    if (encodeModel == null) {
      return null;
    }
    List<Encoder> encoders = encodeModel.getEncoders();
    List<PublicBAOS> publicBAOS = encodeModel.getPublicBAOS();
    while (batchData.hasCurrent()) {
      tsPrimitiveTypeEncode(batchData.currentTsPrimitiveType(), encoders, publicBAOS);
      batchData.next();
    }
    return generateEncodeAndCompressAnalysedBase(encodeModel);
  }

  /**
   * generate encode and compress analysed with tsPrimitiveType array
   *
   * @param tsPrimitiveTypes tsPrimitiveType array
   * @return EncodeCompressAnalysedModel list
   * @throws IOException throw io exception
   */
  public static List<EncodeCompressAnalysedModel> generateEncodeAndCompressAnalysedWithTsPrimitives(
      TsPrimitiveType[] tsPrimitiveTypes) throws IOException {
    //
    DsTypeEncodeModel encodeModel = generateDsTypeEncodeModel(tsPrimitiveTypes[0].getDataType());
    if (encodeModel == null) {
      return null;
    }
    List<Encoder> encoders = encodeModel.getEncoders();
    List<PublicBAOS> publicBAOS = encodeModel.getPublicBAOS();
    for (int i = 0; i < tsPrimitiveTypes.length; i++) {
      tsPrimitiveTypeEncode(tsPrimitiveTypes[i], encoders, publicBAOS);
    }
    return generateEncodeAndCompressAnalysedBase(encodeModel);
  }

  /**
   * generate encode and compress analysed base method
   *
   * @param encodeModel encode model
   * @return EncodeCompressAnalysedModel list
   * @throws IOException throw io exception
   */
  private static List<EncodeCompressAnalysedModel> generateEncodeAndCompressAnalysedBase(
      DsTypeEncodeModel encodeModel) throws IOException {
    List<PublicBAOS> publicBAOS = encodeModel.getPublicBAOS();
    List<String> encodeNameList = encodeModel.getEncodeNameList();
    List<EncodeCompressAnalysedModel> modelList = new ArrayList<>();
    List<Encoder> encoders = encodeModel.getEncoders();
    for (int i = 0; i < encodeModel.getEncoders().size(); i++) {
      encoders.get(i).flush(publicBAOS.get(i));
    }
    long uncompressSize = publicBAOS.get(0).size();
    for (int i = 0; i < encodeModel.getEncoders().size(); i++) {
      for (int j = 0; j < compressTypes.length; j++) {
        modelList.add(
            generateAnalysedModel(
                compressTypes[j],
                encodeNameList.get(i),
                uncompressSize,
                encodeModel.getTypeName(),
                publicBAOS.get(i)));
      }
    }
    for (PublicBAOS baos : publicBAOS) {
      baos.close();
    }
    return modelList;
  }

  /**
   * encode tsPrimitiveType data
   *
   * @param tsPrimitiveType tsPrimitiveType
   * @param encoders encode list
   * @param publicBAOS publicBAOS list
   */
  private static void tsPrimitiveTypeEncode(
      TsPrimitiveType tsPrimitiveType, List<Encoder> encoders, List<PublicBAOS> publicBAOS) {
    switch (tsPrimitiveType.getDataType()) {
      case INT64:
        long longValue = tsPrimitiveType.getLong();
        for (int i = 0; i < encoders.size(); i++) {
          encoders.get(i).encode(longValue, publicBAOS.get(i));
        }
        return;
      case INT32:
        int intValue = tsPrimitiveType.getInt();
        for (int i = 0; i < encoders.size(); i++) {
          encoders.get(i).encode(intValue, publicBAOS.get(i));
        }
        return;
      case FLOAT:
        float floatValue = tsPrimitiveType.getFloat();
        for (int i = 0; i < encoders.size(); i++) {
          encoders.get(i).encode(floatValue, publicBAOS.get(i));
        }
        return;
      case DOUBLE:
        double doubleValue = tsPrimitiveType.getDouble();
        for (int i = 0; i < encoders.size(); i++) {
          encoders.get(i).encode(doubleValue, publicBAOS.get(i));
        }
        return;
      case TEXT:
        Binary textValue = tsPrimitiveType.getBinary();
        for (int i = 0; i < encoders.size(); i++) {
          encoders.get(i).encode(textValue, publicBAOS.get(i));
        }
        return;
      default:
    }
  }

  /**
   * generate dsTypeEncodeModel
   *
   * @param dataType data type
   * @return dsTypeEncodeModel
   */
  private static DsTypeEncodeModel generateDsTypeEncodeModel(TSDataType dataType) {

    switch (dataType) {
      case INT64:
        return generateLongEncodeModel(dataType.name());
      case INT32:
        return generateIntEncodeModel(dataType.name());
      case FLOAT:
        return generateFloatEncodeModel(dataType.name());
      case DOUBLE:
        return generateDoubleEncodeModel(dataType.name());
      case TEXT:
        return generateTextEncodeModel(dataType.name());
      default:
        return null;
    }
  }

  /**
   * generate int encode model
   *
   * @param typeName int type name
   * @return int dsTypeEncodeModel
   */
  private static DsTypeEncodeModel generateIntEncodeModel(String typeName) {
    List<String> encodeNameList = new ArrayList<>();
    encodeNameList.add(TSEncoding.PLAIN.name());
    encodeNameList.add(TSEncoding.GORILLA.name());
    encodeNameList.add(TSEncoding.RLE.name());
    encodeNameList.add(TSEncoding.TS_2DIFF.name());
    PlainEncoder plainEncoder = new PlainEncoder(TSDataType.INT32, 128);
    IntGorillaEncoder gorillaEncoder = new IntGorillaEncoder();
    IntRleEncoder rleEncoder = new IntRleEncoder();
    DeltaBinaryEncoder.IntDeltaEncoder deltaEncoder = new DeltaBinaryEncoder.IntDeltaEncoder();
    List<Encoder> encoders = new ArrayList<>();
    encoders.add(plainEncoder);
    encoders.add(gorillaEncoder);
    encoders.add(rleEncoder);
    encoders.add(deltaEncoder);
    return generateEncodeModel(typeName, encoders, encodeNameList);
  }

  /**
   * generate long encode model
   *
   * @param typeName long type name
   * @return long type dsTypeEncodeModel
   */
  private static DsTypeEncodeModel generateLongEncodeModel(String typeName) {

    List<String> encodeNameList = new ArrayList<>();
    encodeNameList.add(TSEncoding.PLAIN.name());
    encodeNameList.add(TSEncoding.GORILLA.name());
    encodeNameList.add(TSEncoding.RLE.name());
    encodeNameList.add(TSEncoding.TS_2DIFF.name());
    PlainEncoder plainEncoder = new PlainEncoder(TSDataType.INT64, 128);
    LongGorillaEncoder gorillaEncoder = new LongGorillaEncoder();
    LongRleEncoder rleEncoder = new LongRleEncoder();
    DeltaBinaryEncoder.LongDeltaEncoder deltaEncoder = new DeltaBinaryEncoder.LongDeltaEncoder();
    List<Encoder> encoders = new ArrayList<>();
    encoders.add(plainEncoder);
    encoders.add(gorillaEncoder);
    encoders.add(rleEncoder);
    encoders.add(deltaEncoder);
    return generateEncodeModel(typeName, encoders, encodeNameList);
  }

  /**
   * generate float encode model
   *
   * @param typeName float type name
   * @return dsTypeEncodeModel
   */
  private static DsTypeEncodeModel generateFloatEncodeModel(String typeName) {
    List<String> encodeNameList = new ArrayList<>();
    encodeNameList.add(TSEncoding.PLAIN.name());
    encodeNameList.add(TSEncoding.GORILLA.name());
    PlainEncoder plainEncoder = new PlainEncoder(TSDataType.FLOAT, 128);
    SinglePrecisionEncoderV2 gorillaEncoder = new SinglePrecisionEncoderV2();
    List<Encoder> encoders = new ArrayList<>();
    encoders.add(plainEncoder);
    encoders.add(gorillaEncoder);
    return generateEncodeModel(typeName, encoders, encodeNameList);
  }

  /**
   * generate double encode model
   *
   * @param typeName double type name
   * @return dsTypeEncodeModel
   */
  private static DsTypeEncodeModel generateDoubleEncodeModel(String typeName) {
    List<String> encodeNameList = new ArrayList<>();
    encodeNameList.add(TSEncoding.PLAIN.name());
    encodeNameList.add(TSEncoding.GORILLA.name());
    PlainEncoder plainEncoder = new PlainEncoder(TSDataType.DOUBLE, 128);
    DoublePrecisionEncoderV2 gorillaEncoder = new DoublePrecisionEncoderV2();

    List<Encoder> encoders = new ArrayList<>();
    encoders.add(plainEncoder);
    encoders.add(gorillaEncoder);

    return generateEncodeModel(typeName, encoders, encodeNameList);
  }

  /**
   * generate text encode model
   *
   * @param typeName text type name
   * @return dsTypeEncodeModel
   */
  private static DsTypeEncodeModel generateTextEncodeModel(String typeName) {
    List<String> encodeNameList = new ArrayList<>();
    encodeNameList.add(TSEncoding.PLAIN.name());
    encodeNameList.add(TSEncoding.DICTIONARY.name());
    PlainEncoder plainEncoder = new PlainEncoder(TSDataType.TEXT, 128);
    DictionaryEncoder dictionaryEncoder = new DictionaryEncoder();
    List<Encoder> encoders = new ArrayList<>();
    encoders.add(plainEncoder);
    encoders.add(dictionaryEncoder);
    return generateEncodeModel(typeName, encoders, encodeNameList);
  }

  /**
   * @param typeName type name
   * @param encoders encode list
   * @param encodeNameList encodeName list
   * @return dsTypeEncodeModel
   */
  private static DsTypeEncodeModel generateEncodeModel(
      String typeName, List<Encoder> encoders, List<String> encodeNameList) {
    DsTypeEncodeModel model = new DsTypeEncodeModel();
    model.setTypeName(typeName);

    List<PublicBAOS> baos = new ArrayList<>();
    for (int i = 0; i < encoders.size(); i++) {
      baos.add(new PublicBAOS());
    }
    model.setEncodeNameList(encodeNameList);
    model.setEncoders(encoders);
    model.setPublicBAOS(baos);
    return model;
  }

  /**
   * 组装EncodeCompressAnalysedModel模型
   *
   * @param compressionType compress type
   * @param encodeName encode name
   * @param originSize origin size
   * @param typeName type name
   * @param baos baos
   * @return encode compress analysed model
   * @throws IOException throw io exception
   */
  private static EncodeCompressAnalysedModel generateAnalysedModel(
      CompressionType compressionType,
      String encodeName,
      long originSize,
      String typeName,
      PublicBAOS baos)
      throws IOException {
    ICompressor compressor;
    if (compressionType.equals(CompressionType.SNAPPY)) {
      compressor = new ICompressor.SnappyCompressor();
    } else if (compressionType.equals(CompressionType.GZIP)) {
      compressor = new ICompressor.GZIPCompressor();
    } else if (compressionType.equals(CompressionType.LZ4)) {
      compressor = new ICompressor.IOTDBLZ4Compressor();
    } else {
      compressor = new ICompressor.NoCompressor();
    }
    long startTime = System.nanoTime();
    long compressedSize = compressor.compress(baos.getBuf()).length;
    long compressedCost = System.nanoTime() - startTime;
    EncodeCompressAnalysedModel model = new EncodeCompressAnalysedModel();
    model.setCompressName(compressionType.name());
    model.setCompressedSize(compressedSize);
    model.setUncompressSize(baos.size());
    model.setTypeName(typeName);
    model.setEncodeName(encodeName);
    model.setEncodedSize(baos.size());
    model.setOriginSize(originSize);
    model.setCompressedCost(compressedCost);
    return model;
  }

  /**
   * sorted analysed model
   *
   * @param map encodeCompressAnalysedModel map
   * @return EncodeCompressAnalysedModel list
   */
  public static List<EncodeCompressAnalysedModel> sortedAnalysedModel(
      Map<String, EncodeCompressAnalysedModel> map) {
    List<EncodeCompressAnalysedModel> sortedCostModels =
        map.values().stream()
            .sorted(Comparator.comparing(EncodeCompressAnalysedModel::getCompressedCost))
            .collect(Collectors.toList());
    List<EncodeCompressAnalysedModel> sortedCompressedModels =
        map.values().stream()
            .sorted(Comparator.comparing(EncodeCompressAnalysedModel::getCompressedSize))
            .collect(Collectors.toList());
    Map<String, EncodeCompressAnalysedModel> scoresMap = new HashMap<>();
    // 计算压缩得分
    for (int i = 0; i < sortedCompressedModels.size(); i++) {
      EncodeCompressAnalysedModel model = sortedCompressedModels.get(i);
      double compressedScores =
          compressedWeight * (1 - (double) model.getCompressedSize() / model.getOriginSize());
      double sequenceScores = 0;
      double rate = (double) i / sortedCostModels.size();
      if (rate < zeroRate) {
        sequenceScores = compressedSequenceWeight * (1 - rate);
      }
      model.setScore(compressedScores + sequenceScores);
      String key = model.getCompressName() + "-" + model.getEncodeName();
      scoresMap.put(key, model);
    }
    // 计算耗时得分
    for (int i = 0; i < sortedCostModels.size(); i++) {
      EncodeCompressAnalysedModel model = sortedCostModels.get(i);
      double sequenceScores = 0;
      double rate = (double) i / sortedCostModels.size();
      if (rate < zeroRate) {
        sequenceScores = compressedCostWeight * (1 - rate);
      }
      String key = model.getCompressName() + "-" + model.getEncodeName();
      scoresMap.get(key).setScore(scoresMap.get(key).getScore() + sequenceScores);
    }
    return scoresMap.values().stream()
        .sorted(Comparator.comparing(EncodeCompressAnalysedModel::getScore).reversed())
        .collect(Collectors.toList());
  }
}
