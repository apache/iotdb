package org.apache.iotdb.tsfile.utils;

import com.google.common.primitives.UnsignedBytes;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

// based on KLL Sketch in DataSketch. See
// https://github.com/apache/datasketches-java/tree/master/src/main/java/org/apache/datasketches/kll
// This is an implementation for long data type.
// We changed the behaviour of serialization to reduce the serialization size. In our situation,
// there will be few update operations after deserialization.
public class LongKLLSketch extends KLLSketchForQuantile {
  int maxN, maxSerializeNum;
  int K;
  int maxLevel;

  public LongKLLSketch(
      int maxN, int maxMemoryByte, int maxSerializeByte) { // maxN=7000 for PAGE, 1.6e6 for CHUNK
    this.maxN = maxN;
    N = 0;
    maxLevel = calcMaxLevel(maxN, maxSerializeByte);
    calcParameters(maxMemoryByte, maxSerializeByte);
    calcLevelMaxSize(1);
  }

  //  public int getCntLevel(){return cntLevel;}
  //  public int[] getLevelPos(){return levelPos;}
  //  public long[] getNum(){return num;}
  private void calcParameters(int maxMemoryByte, int maxSerializeByte) {
    K = calcK(maxN, maxLevel);
    maxMemoryNum = calcMaxMemoryNum(maxMemoryByte);
    maxSerializeNum = calcMaxSerializeNum(maxSerializeByte);
    num = new long[maxMemoryNum];
    level0Sorted = false;
    levelPos = new int[maxLevel + 1];
    for (int i = 0; i < maxLevel + 1; i++) levelPos[i] = maxMemoryNum;
    cntLevel = 0;
  }

  private int calcMaxLevel(int maxN, int maxSerializeByte) {
    return (int) Math.ceil(Math.log(maxN / (maxSerializeByte / 8.0)) / Math.log(2)) + 1;
  }

  private int calcK(int maxN, int maxLevel) {
    return maxN / (1 << (maxLevel - 1));
  }

  private int calcMaxSerializeNum(int maxSerializeByte) {
    return maxSerializeByte / 8;
    //    return (maxSerializeByte - 15 - maxLevel * (maxSerializeByte / 8 < 256 ? 1 : 2)) / 8;
  }

  @Override
  protected int calcMaxMemoryNum(int maxMemoryByte) {
    return Math.min(maxN, Math.min(1 << 20, maxMemoryByte / 8));
    //    return Math.min(maxN, Math.min(1 << 20, maxMemoryByte / 8 - maxLevel * 2 - 5));
  }

  @Override
  protected void calcLevelMaxSize(int setLevel) { // set cntLevel.  cntLevel won't decrease
    cntLevel = setLevel;
    levelMaxSize = new int[cntLevel];
    int newK = K;
    for (int addK = 1 << 28; addK > 0; addK >>>= 1) {
      int need = 0;
      for (int i = 0; i < cntLevel; i++)
        need +=
            Math.min(
                K << (maxLevel - i - 1),
                Math.max(
                    8, (int) Math.round(((newK + addK) * Math.pow(2.0 / 3, maxLevel - i - 1)))));
      if (need <= maxMemoryNum) newK += addK;
    }
    for (int i = 0; i < cntLevel; i++)
      levelMaxSize[i] =
          Math.min(
              K << (maxLevel - i - 1),
              Math.max(8, (int) Math.round((newK * Math.pow(2.0 / 3, maxLevel - i - 1)))));
    //    show();
  }

  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append(N);
    sb.append(maxN);
    sb.append((byte) maxLevel);
    sb.append((short) (levelPos[maxLevel] - levelPos[0]));
    for (int i = 0; i < maxLevel; i++) sb.append(levelPos[i]);
    return "";
  }

  public void showLevelMaxSize() {
    int numLEN = getNumLen();
    System.out.println(
        "\t\tCOMPACT_SIZE:"
            + (13 + (maxLevel) * (numLEN < 256 ? 1 : 2) + numLEN * 8)
            + "\t//maxMemNum:"
            + maxMemoryNum
            + ",maxSeriNum:"
            + maxSerializeNum
            + "\t//N:"
            + N);
    for (int i = 0; i < cntLevel; i++) System.out.print("\t\t" + levelMaxSize[i] + "\t");
    System.out.println();
    System.out.println("-------------------------------------------------------");
  }

  private void compactOneLevel(
      int level, int numToReduce) { // compact half of data when numToReduce is small
    if (numToReduce <= 0) return;
    if (level == cntLevel - 1) calcLevelMaxSize(cntLevel + 1);
    int L1 = levelPos[level], R1 = levelPos[level + 1]; // [L,R)
    if (level == 0 && !level0Sorted) {
      Arrays.sort(num, L1, R1);
      level0Sorted = true;
    }
    L1 += (R1 - L1) & 1;
    if (L1 == R1) return;
    //    boolean FLAG = false;
    if ((R1 - L1) / 2 > numToReduce) {
      L1 = R1 - numToReduce * 2;
      //      FLAG=true;
      //      System.out.println("----------------------------");show();
    }

    randomlyHalveDownToLeft(L1, R1);

    int mid = (L1 + R1) >>> 1;
    mergeSortWithoutSpace(L1, mid, levelPos[level + 1], levelPos[level + 2]);
    levelPos[level + 1] = mid;
    int newP = levelPos[level + 1] - 1, oldP = L1 - 1;
    for (int i = oldP; i >= levelPos[0]; i--) num[newP--] = num[oldP--];

    levelPos[level] = levelPos[level + 1] - (L1 - levelPos[level]);
    int numReduced = (R1 - L1) >>> 1;
    for (int i = level - 1; i >= 0; i--) levelPos[i] += numReduced;
    //    if(FLAG){
    //      show();
    //      System.out.println("----------------------------");
    //    }
  }

  @Override
  public void compact() {
    int compactLevel = cntLevel - 1;
    for (int i = 0; i < cntLevel; i++)
      if (levelPos[i + 1] - levelPos[i] > levelMaxSize[i]) {
        compactLevel = i;
        break;
      }
    if (compactLevel == maxLevel - 1) {
      if (levelPos[0] == 0) System.out.println("\t\t[MYKLL ERROR] compactLevel wrong!");
      return;
    }
    compactOneLevel(compactLevel, Integer.MAX_VALUE);
  }

  public void compactBeforeSerialization() {
    for (int i = 0; i < cntLevel - 1; i++) {
      if (levelPos[maxLevel] - levelPos[0] <= maxSerializeNum) break;
      if (levelPos[i + 1] - levelPos[i] >= levelMaxSize[i])
        compactOneLevel(i, levelPos[maxLevel] - levelPos[0] - maxSerializeNum);
    }
    for (int i = 0; i < maxLevel - 1; i++) {
      if (levelPos[maxLevel] - levelPos[0] <= maxSerializeNum) break;
      if (levelPos[i + 1] - levelPos[i] >= 2)
        compactOneLevel(i, levelPos[maxLevel] - levelPos[0] - maxSerializeNum);
    }
  }

  public int serialize(OutputStream outputStream) throws IOException { // 15+1*?+8*?
    compactBeforeSerialization(); // if N==maxN
    int byteLen = 0;
    byteLen += ReadWriteIOUtils.write(N, outputStream);
    byteLen += ReadWriteIOUtils.write(maxN, outputStream);
    byteLen += ReadWriteIOUtils.write((byte) maxLevel, outputStream);
    int numLEN = getNumLen();
    byteLen += ReadWriteIOUtils.write((short) numLEN, outputStream);
    if (numLEN < 256)
      for (int i = 0; i < maxLevel; i++)
        byteLen += ReadWriteIOUtils.write((byte) (levelPos[i + 1] - levelPos[i]), outputStream);
    else
      for (int i = 0; i < maxLevel; i++)
        byteLen += ReadWriteIOUtils.write((short) (levelPos[i + 1] - levelPos[i]), outputStream);
    for (int i = levelPos[0]; i < levelPos[maxLevel]; i++)
      byteLen += ReadWriteIOUtils.write(num[i], outputStream);
    return byteLen;
  }

  public LongKLLSketch(InputStream inputStream, int maxMemoryByte, int maxSerializeByte)
      throws IOException {
    this.N = ReadWriteIOUtils.readLong(inputStream);
    this.maxN = ReadWriteIOUtils.readInt(inputStream);
    this.maxLevel = ReadWriteIOUtils.readByte(inputStream);
    calcParameters(maxMemoryByte, maxSerializeByte);
    int numLEN = ReadWriteIOUtils.readShort(inputStream);
    for (int i = 0, tmp = 0; i < maxLevel; i++) {
      levelPos[i] = maxMemoryNum - (numLEN - tmp);
      if (numLEN < 256) tmp += UnsignedBytes.toInt(ReadWriteIOUtils.readByte(inputStream));
      else tmp += ReadWriteIOUtils.readShort(inputStream);
    }
    int actualLevel = maxLevel - 1;
    while (levelPos[actualLevel] == levelPos[actualLevel + 1]) actualLevel--;
    calcLevelMaxSize(actualLevel + 1);
    for (int i = 0; i < numLEN; i++)
      num[maxMemoryNum - numLEN + i] = ReadWriteIOUtils.readLong(inputStream);
  }

  public LongKLLSketch(ByteBuffer byteBuffer, int maxMemoryByte, int maxSerializeByte) {
    this.N = ReadWriteIOUtils.readLong(byteBuffer);
    this.maxN = ReadWriteIOUtils.readInt(byteBuffer);
    this.maxLevel = ReadWriteIOUtils.readByte(byteBuffer);
    calcParameters(maxMemoryByte, maxSerializeByte);
    int numLEN = ReadWriteIOUtils.readShort(byteBuffer);
    for (int i = 0, tmp = 0; i < maxLevel; i++) {
      levelPos[i] = maxMemoryNum - (numLEN - tmp);
      if (numLEN < 256) tmp += UnsignedBytes.toInt(ReadWriteIOUtils.readByte(byteBuffer));
      else tmp += ReadWriteIOUtils.readShort(byteBuffer);
    }
    int actualLevel = maxLevel - 1;
    while (levelPos[actualLevel] == levelPos[actualLevel + 1]) actualLevel--;
    calcLevelMaxSize(actualLevel + 1);
    for (int i = 0; i < numLEN; i++)
      num[maxMemoryNum - numLEN + i] = ReadWriteIOUtils.readLong(byteBuffer);
  }

  public LongKLLSketch(InputStream inputStream) throws IOException {
    this.N = ReadWriteIOUtils.readLong(inputStream);
    this.maxN = ReadWriteIOUtils.readInt(inputStream);
    this.maxLevel = ReadWriteIOUtils.readByte(inputStream);
    int numLEN = ReadWriteIOUtils.readShort(inputStream);
    this.maxSerializeNum = numLEN;

    K = calcK(maxN, maxLevel);
    maxMemoryNum = numLEN;
    num = new long[maxMemoryNum];
    level0Sorted = false;
    levelPos = new int[maxLevel + 1];
    levelPos[maxLevel] = numLEN;

    for (int i = 0, tmp = 0; i < maxLevel; i++) {
      //      System.out.println("\t\ttmp:"+tmp+"\t\tnumLen:"+numLEN+"\t\tmemMemNum:"+maxMemoryNum);
      levelPos[i] = maxMemoryNum - (numLEN - tmp);
      if (numLEN < 256) tmp += UnsignedBytes.toInt(ReadWriteIOUtils.readByte(inputStream));
      else tmp += ReadWriteIOUtils.readShort(inputStream);
    }
    int actualLevel = maxLevel - 1;
    while (levelPos[actualLevel] == levelPos[actualLevel + 1]) actualLevel--;
    calcLevelMaxSize(actualLevel + 1);
    for (int i = 0; i < numLEN; i++)
      num[maxMemoryNum - numLEN + i] = ReadWriteIOUtils.readLong(inputStream);
  }

  public LongKLLSketch(ByteBuffer byteBuffer) {
    this.N = ReadWriteIOUtils.readLong(byteBuffer);
    this.maxN = ReadWriteIOUtils.readInt(byteBuffer);
    this.maxLevel = ReadWriteIOUtils.readByte(byteBuffer);

    int numLEN = ReadWriteIOUtils.readShort(byteBuffer);
    maxSerializeNum = numLEN;

    K = calcK(maxN, maxLevel);
    maxMemoryNum = numLEN;
    num = new long[maxMemoryNum];
    level0Sorted = false;
    levelPos = new int[maxLevel + 1];
    levelPos[maxLevel] = numLEN;

    for (int i = 0, tmp = 0; i < maxLevel; i++) {
      levelPos[i] = maxMemoryNum - (numLEN - tmp);
      if (numLEN < 256) tmp += UnsignedBytes.toInt(ReadWriteIOUtils.readByte(byteBuffer));
      else tmp += ReadWriteIOUtils.readShort(byteBuffer);
    }
    int actualLevel = maxLevel - 1;
    while (levelPos[actualLevel] == levelPos[actualLevel + 1]) actualLevel--;
    calcLevelMaxSize(actualLevel + 1);
    for (int i = 0; i < numLEN; i++)
      num[maxMemoryNum - numLEN + i] = ReadWriteIOUtils.readLong(byteBuffer);
  }
}
