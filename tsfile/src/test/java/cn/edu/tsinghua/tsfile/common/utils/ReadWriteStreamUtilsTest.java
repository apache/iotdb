package cn.edu.tsinghua.tsfile.common.utils;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ReadWriteStreamUtilsTest {
  private List<Integer> unsignedVarIntList;
  private List<Integer> littleEndianIntList;
  private List<Long> littleEndianLongList;

  @Before
  public void setUp() throws Exception {
    unsignedVarIntList = new ArrayList<Integer>();
    littleEndianIntList = new ArrayList<Integer>();
    littleEndianLongList = new ArrayList<Long>();

    int uvInt = 123;
    for (int i = 0; i < 10; i++) {
      unsignedVarIntList.add(uvInt);
      unsignedVarIntList.add(uvInt - 1);
      uvInt *= 3;
    }

    int leInt = 17;
    for (int i = 0; i < 17; i++) {
      littleEndianIntList.add(leInt);
      littleEndianIntList.add(leInt - 1);
      leInt *= 3;
    }

    long leLong = 13;
    for (int i = 0; i < 38; i++) {
      littleEndianLongList.add(leLong);
      littleEndianLongList.add(leLong - 1);
      leLong *= 3;
    }
  }

  @After
  public void tearDown() throws Exception {}

  @Test
  public void testGetIntMinBitWidth() {
    List<Integer> uvIntList = new ArrayList<Integer>();
    uvIntList.add(0);
    assertEquals(1, ReadWriteStreamUtils.getIntMaxBitWidth(uvIntList));
    uvIntList.add(1);
    assertEquals(1, ReadWriteStreamUtils.getIntMaxBitWidth(uvIntList));
    int uvInt = 123;
    for (int i = 0; i < 10; i++) {
      uvIntList.add(uvInt);
      uvIntList.add(uvInt - 1);
      assertEquals(32 - Integer.numberOfLeadingZeros(uvInt),
          ReadWriteStreamUtils.getIntMaxBitWidth(uvIntList));
      uvInt *= 3;
    }
  }

  @Test
  public void testGetLongMinBitWidth() {
    List<Long> uvLongList = new ArrayList<Long>();
    uvLongList.add(0L);
    assertEquals(1, ReadWriteStreamUtils.getLongMaxBitWidth(uvLongList));
    uvLongList.add(1L);
    assertEquals(1, ReadWriteStreamUtils.getLongMaxBitWidth(uvLongList));
    long uvLong = 123;
    for (int i = 0; i < 10; i++) {
      uvLongList.add(uvLong);
      uvLongList.add(uvLong - 1);
      assertEquals(64 - Long.numberOfLeadingZeros(uvLong),
          ReadWriteStreamUtils.getLongMaxBitWidth(uvLongList));
      uvLong *= 7;
    }
  }

  @Test
  public void testReadUnsignedVarInt() throws IOException {
    for (int uVarInt : unsignedVarIntList) {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ReadWriteStreamUtils.writeUnsignedVarInt(uVarInt, baos);
      ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
      int value_read = ReadWriteStreamUtils.readUnsignedVarInt(bais);
      assertEquals(value_read, uVarInt);
    }
  }

  /**
   * @see {@link #testReadUnsignedVarInt()}
   */
  @Test
  public void testWriteUnsignedVarInt() {}

  /**
   * @see {@link #testReadIntLittleEndianPaddedOnBitWidth()}
   */
  @Test
  public void testWriteIntLittleEndianPaddedOnBitWidth() {}

  /**
   * @see {@link #testReadLongLittleEndianPaddedOnBitWidth()}
   */
  @Test
  public void testWriteLongLittleEndianPaddedOnBitWidth() {}

  @Test
  public void testReadIntLittleEndianPaddedOnBitWidth() throws IOException {
    for (int value : littleEndianIntList) {
      int bitWidth = 32 - Integer.numberOfLeadingZeros(value);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ReadWriteStreamUtils.writeIntLittleEndianPaddedOnBitWidth(value, baos, bitWidth);
      ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());

      int value_read = ReadWriteStreamUtils.readIntLittleEndianPaddedOnBitWidth(bais, bitWidth);
      // System.out.println(bitWidth+"/"+value_read+"/"+value);
      assertEquals(value_read, value);
    }
  }

  @Test
  public void testReadLongLittleEndianPaddedOnBitWidth() throws IOException {
    for (long value : littleEndianLongList) {
      int bitWidth = 64 - Long.numberOfLeadingZeros(value);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ReadWriteStreamUtils.writeLongLittleEndianPaddedOnBitWidth(value, baos, bitWidth);
      ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());

      long value_read = ReadWriteStreamUtils.readLongLittleEndianPaddedOnBitWidth(bais, bitWidth);
      // System.out.println(bitWidth+"/"+value_read+"/"+value);
      assertEquals(value_read, value);
    }
  }

}
