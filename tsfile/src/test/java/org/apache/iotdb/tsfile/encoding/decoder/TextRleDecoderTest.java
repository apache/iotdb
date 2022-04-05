package org.apache.iotdb.tsfile.encoding.decoder;

import org.apache.iotdb.tsfile.encoding.encoder.TextRleEncoder;
import org.apache.iotdb.tsfile.utils.Binary;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TextRleDecoderTest {
  private List<Binary> rleList;
  private List<Binary> bpList;

  @Before
  public void setUp() {
    rleList = new ArrayList<>();
    int rleCount = 11;
    int rleNum = 38;
    String textStr1 = "This is a string";
    Binary rleStart = new Binary(textStr1);
    for (int i = 0; i < rleNum; i++) {
      for (int j = 0; j < rleCount; j++) {
        rleList.add(rleStart);
      }
      for (int j = 0; j < rleCount; j++) {
        rleList.add(new Binary(textStr1 + 1));
      }
      rleCount += 2;
    }
    bpList = new ArrayList<>();
    int bpCount = 15;
    String textStr2 = "Thatisateststring2.";
    Binary bpStart = new Binary(textStr2);
    for (int i = 0; i < bpCount; i++) {
      textStr2 += 3;
      if (i % 2 == 1) {
        bpList.add(new Binary(textStr2));
      } else {
        bpList.add(bpStart);
      }
    }
  }

  @After
  public void tearDown() {}

  @Test
  public void testSingleText() throws IOException {
    Binary text = new Binary("123456789101");
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    TextRleEncoder encoder = new TextRleEncoder();
    encoder.encode(text, baos);
    encoder.flush(baos);
    ByteBuffer buffer = ByteBuffer.wrap(baos.toByteArray());
    TextRleDecoder decoder = new TextRleDecoder();
    Binary text2 = decoder.readBinary(buffer);
    assertEquals(text, text2);
  }

  @Test
  public void testRleReadLong() throws IOException {
    for (int i = 1; i < 2; i++) {
      testLength(rleList, false, i);
    }
  }

  @Test
  public void testMaxRLERepeatNUM() throws IOException {
    List<Binary> repeatList = new ArrayList<>();
    int rleCount = 17;
    int rleNum = 5;
    long rleStart = 11;
    for (int i = 0; i < rleNum; i++) {
      for (int j = 0; j < rleCount; j++) {
        repeatList.add(new Binary(String.valueOf(rleStart)));
      }
      for (int j = 0; j < rleCount; j++) {
        repeatList.add(new Binary(String.valueOf(rleStart / 3)));
      }
      rleCount *= 7;
      rleStart *= -3;
    }
    for (int i = 1; i < 10; i++) {
      testLength(repeatList, false, i);
    }
  }

  @Test
  public void testBitPackingReadLong() throws IOException {
    for (int i = 1; i < 10; i++) {
      testLength(bpList, false, i);
    }
  }

  public void testLength(List<Binary> list, boolean isDebug, int repeatCount) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    TextRleEncoder encoder = new TextRleEncoder();
    for (int i = 0; i < repeatCount; i++) {
      for (Binary value : list) {
        encoder.encode(value, baos);
      }
      encoder.flush(baos);
    }

    ByteBuffer buffer = ByteBuffer.wrap(baos.toByteArray());
    TextRleDecoder decoder = new TextRleDecoder();
    for (int i = 0; i < repeatCount; i++) {
      for (Binary value : list) {
        Binary value_ = decoder.readBinary(buffer);
        if (isDebug) {
          System.out.println(value_ + "/" + value);
        }
        assertEquals(value, value_);
      }
    }
  }
}
