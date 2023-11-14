package org.apache.iotdb.tsfile.encoding;

import com.kamikaze.pfordelta.Simple16;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

public class myPFOR {
  // NOTE: we expect the blockSize is always < (1<<(31-POSSIBLE_B_BITS)).
  // For example, in the current default settings,
  // the blockSize < (1<<(31-5)), that is, < 2^27, the commonly used block
  // size is 128 or 256.

  // All possible values of b in the PForDelta algorithm
  private static final int[] POSSIBLE_B = {
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 16, 20, 28
  };
  private static final int MAX_BITS = 32;

  private static final int HEADER_NUM = 4;
  private static final int HEADER_SIZE = MAX_BITS * HEADER_NUM;
  private static int min = Integer.MAX_VALUE;

  public static int[] compressOneBlockOpt(final int[] inBlock, int blockSize) {
    // find the best b that may lead to the smallest overall
    // compressed size
    int[] inBlock2 = new int[blockSize];
    min = Integer.MAX_VALUE;
    for (int k : inBlock) {
      if (k < min) {
        min = k;
      }
    }
    for (int j = 0; j < inBlock.length; j++) {
      inBlock2[j] = inBlock[j] - min;
    }
    int currentB = POSSIBLE_B[0];
    int[] outBlock = null;
    int tmpB = currentB;
    int optSize = estimateCompressedSize(inBlock2, tmpB, blockSize);
    for (int i = 1; i < POSSIBLE_B.length; ++i) {
      tmpB = POSSIBLE_B[i];
      int curSize = estimateCompressedSize(inBlock2, tmpB, blockSize);
      if (curSize < optSize) {
        currentB = tmpB;
        optSize = curSize;
      }
    }
    // 使用相同的b分block压缩
    outBlock = compressOneBlock(inBlock2, currentB, blockSize);

    return outBlock;
  }

  public static int decompressOneBlock(int[] outBlock, final int[] inBlock, int blockSize) {
    int[] expAux = new int[blockSize];

    int expNum = inBlock[1];
    int bits = inBlock[0];
    int index_first_exp = inBlock[2];
    min = inBlock[3];

    int offset = HEADER_SIZE;
    int compressedBits = 0;
    if (bits == 0) {
      Arrays.fill(outBlock, 0);
    } else {

      compressedBits = decompressBBitSlots(outBlock, inBlock, blockSize, bits);
    }
    offset += compressedBits;

    if (expNum > 0) {
      compressedBits = decompressBlockByS16(expAux, inBlock, offset, expNum);
      offset += compressedBits;

      int exp_index = index_first_exp;
      int shift;
      for (int i = 0; i < expNum; i++) {
        int exp_value = expAux[i];
        shift = outBlock[exp_index];
        outBlock[exp_index] = exp_value;
        exp_index += (shift + 1);
      }
    }

    for (int i = 0; i < blockSize; i++) {
      outBlock[i] += min;
    }
    return offset;
  }

  public static int estimateCompressedSize(int[] inputBlock, int bits, int blockSize) {
    int maxNoExp = (1 << bits) - 1;
    // Size of the header and the bits-bit slots
    int outputOffset = HEADER_SIZE + bits * blockSize;
    int expNum = 0;

    for (int i = 0; i < blockSize; ++i) {
      if (inputBlock[i] > maxNoExp) {
        expNum++;
      }
    }
    outputOffset += (expNum << 5);

    return outputOffset;
  }

  public static int[] compressOneBlock(int[] inputBlock, int bits, int blockSize) {
    int maxCompBitSize = HEADER_SIZE + blockSize * (MAX_BITS + MAX_BITS + MAX_BITS) + 32;

    int[] tmpCompressedBlock = new int[(maxCompBitSize >>> 5)];

    int outputOffset = HEADER_SIZE;
    int expUpperBound = 1 << bits;
    int expNum = 0;
    int expNum_more = 0;
    ArrayList<Integer> exp_index = new ArrayList<>();
    ArrayList<Integer> exp_index_more = new ArrayList<>();
    ArrayList<Integer> shift = new ArrayList<>();
    ArrayList<Integer> exp_value = new ArrayList<>();
    ArrayList<Integer> exp_value_more = new ArrayList<>();
    for (int i = 0; i < inputBlock.length; i++) {
      if (inputBlock[i] >= expUpperBound) {
        expNum++;
        exp_value.add(inputBlock[i]);
        exp_index.add(i);
      }
    }

    if (expNum == 0) {

      for (int i = 0; i < blockSize; i++) {
        writeBits(tmpCompressedBlock, inputBlock[i], outputOffset, bits);
        outputOffset += bits;
      }
    } else if (expNum == 1) {

      for (int i = 0; i < blockSize; i++) {
        if (i != exp_index.get(0)) {
          writeBits(tmpCompressedBlock, inputBlock[i], outputOffset, bits);
        } else {
          writeBits(tmpCompressedBlock, 0, outputOffset, bits);
        }
        outputOffset += bits;
      }
    } else {

      exp_index_more.add(exp_index.get(0));
      exp_value_more.add(exp_value.get(0));
      expNum_more++;
      for (int i = 0; i < expNum - 1; i++) {
        if (exp_index.get(i + 1) - exp_index.get(i) <= (1 << bits)) {

          exp_index_more.add(exp_index.get(i + 1));
          exp_value_more.add(exp_value.get(i + 1));
          expNum_more++;
          shift.add(exp_index.get(i + 1) - exp_index.get(i) - 1);
        } else {

          int tag = exp_index.get(i);
          while (exp_index.get(i + 1) - tag > (1 << bits)) {

            tag += (1 << bits);
            expNum_more++;
            exp_index_more.add(tag);
            exp_value_more.add(inputBlock[tag]);
            shift.add((1 << bits) - 1);
          }

          exp_index_more.add(exp_index.get(i + 1));
          exp_value_more.add(exp_value.get(i + 1));
          expNum_more++;
          shift.add(exp_index.get(i + 1) - tag - 1);
        }
      }
      shift.add(0);

      int j = 0;
      for (int i = 0; i < blockSize; i++) {
        if (j < expNum_more && exp_index_more.get(j) == i) {

          writeBits(tmpCompressedBlock, shift.get(j), outputOffset, bits);
          j++;
        } else {

          writeBits(tmpCompressedBlock, inputBlock[i], outputOffset, bits);
        }
        outputOffset += bits;
      }
    }

    tmpCompressedBlock[0] = bits;
    tmpCompressedBlock[1] = expNum;
    if (tmpCompressedBlock[1] > 1) {
      tmpCompressedBlock[1] = expNum_more;
    }
    if (expNum > 0) {
      tmpCompressedBlock[2] = exp_index.get(0);
    } else {
      tmpCompressedBlock[2] = 0;
    }
    tmpCompressedBlock[3] = min;

    if (expNum == 1) {
      int[] expAux = new int[1];
      expAux[0] = exp_value.get(0);
      int compressedBitSize = compressBlockByS16(tmpCompressedBlock, outputOffset, expAux, expNum);
      outputOffset += compressedBitSize;
    } else if (expNum > 1) {

      int[] expAux = new int[expNum_more];
      for (int i = 0; i < expNum_more; i++) {
        expAux[i] = exp_value_more.get(i);
      }
      int compressedBitSize =
          compressBlockByS16(tmpCompressedBlock, outputOffset, expAux, expNum_more);
      outputOffset += compressedBitSize;
    }

    int compressedSizeInInts = (outputOffset + 31) >>> 5;
    int[] compBlock;
    compBlock = new int[compressedSizeInInts];
    System.arraycopy(tmpCompressedBlock, 0, compBlock, 0, compressedSizeInInts);

    return compBlock;
  }

  public static int decompressBBitSlots(
      int[] outDecompSlots, int[] inCompBlock, int blockSize, int bits) {
    int compressedBitSize = 0;
    int offset = HEADER_SIZE;
    for (int i = 0; i < blockSize; i++) {
      outDecompSlots[i] = readBits(inCompBlock, offset, bits);
      offset += bits;
    }
    compressedBitSize = bits * blockSize;

    return compressedBitSize;
  }

  private static int compressBlockByS16(
      int[] outCompBlock, int outStartOffsetInBits, int[] inBlock, int blockSize) {
    int outOffset = (outStartOffsetInBits + 31) >>> 5;
    int num, inOffset = 0, numLeft;
    for (numLeft = blockSize; numLeft > 0; numLeft -= num) {
      num = Simple16.s16Compress(outCompBlock, outOffset, inBlock, inOffset, numLeft, blockSize);
      outOffset++;
      inOffset += num;
    }
    int compressedBitSize = (outOffset << 5) - outStartOffsetInBits;
    return compressedBitSize;
  }

  public static int decompressBlockByS16(
      int[] outDecompBlock, int[] inCompBlock, int inStartOffsetInBits, int blockSize) {
    int inOffset = (inStartOffsetInBits + 31) >>> 5;
    int num, outOffset = 0, numLeft;
    for (numLeft = blockSize; numLeft > 0; numLeft -= num) {
      num = Simple16.s16Decompress(outDecompBlock, outOffset, inCompBlock, inOffset, numLeft);
      outOffset += num;
      inOffset++;
    }
    int compressedBitSize = (inOffset << 5) - inStartOffsetInBits;
    return compressedBitSize;
  }

  public static void writeBits(int[] out, int val, int outOffset, int bits) {
    if (bits == 0) return;
    final int index = outOffset >>> 5;
    final int skip = outOffset & 0x1f;
    val &= (0xffffffff >>> (32 - bits));
    out[index] |= (val << skip);
    if (32 - skip < bits) {
      out[index + 1] |= (val >>> (32 - skip));
    }
  }

  public static int readBits(int[] in, final int inOffset, final int bits) {
    final int index = inOffset >>> 5;
    final int skip = inOffset & 0x1f;
    int val = in[index] >>> skip;
    if (32 - skip < bits) {
      val |= (in[index + 1] << (32 - skip));
    }
    return val & (0xffffffff >>> (32 - bits));
  }

  public static void main(@NotNull String[] args) throws IOException {
    int length = 75809;
    int width = 10000000;
    int[] timeSeries = new int[length];
    Random rand = new Random();

    for (int i = 0; i < length; i++) {
      timeSeries[i] = rand.nextInt(width) - width / 2;
    }
    int[] outBlock;
    outBlock = compressOneBlockOpt(timeSeries, length);
    int[] uncompressed = new int[length];
    int size = decompressOneBlock(uncompressed, outBlock, length);
    System.out.println(size);
    System.out.println(uncompressed.length);
    for (int i = 0; i < length; i++) {
      if (timeSeries[i] != uncompressed[i]) {
        System.out.println(i);
      }
    }
  }
}
