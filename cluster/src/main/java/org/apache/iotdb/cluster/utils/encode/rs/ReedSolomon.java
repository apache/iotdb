/**
 * Reed-Solomon Coding over 8-bit values.
 *
 * <p>Copyright 2015, Backblaze, Inc.
 */
package org.apache.iotdb.cluster.utils.encode.rs;

/** Reed-Solomon Coding over 8-bit values. */
public class ReedSolomon {

  private final int dataShardCount;
  private final int parityShardCount;
  private final int totalShardCount;
  private final Matrix matrix;

  /**
   * Rows from the matrix for encoding parity, each one as its own byte array to allow for efficient
   * access while encoding.
   */
  private final byte[][] parityRows;

  /** Initializes a new encoder/decoder. */
  public ReedSolomon(int dataShardCount, int parityShardCount) {
    this.dataShardCount = dataShardCount;
    this.parityShardCount = parityShardCount;
    this.totalShardCount = dataShardCount + parityShardCount;
    matrix = buildMatrix(dataShardCount, this.totalShardCount);
    parityRows = new byte[parityShardCount][];
    for (int i = 0; i < parityShardCount; i++) {
      parityRows[i] = matrix.getRow(dataShardCount + i);
    }
  }

  /** Returns the number of data shards. */
  public int getDataShardCount() {
    return dataShardCount;
  }

  /** Returns the number of parity shards. */
  public int getParityShardCount() {
    return parityShardCount;
  }

  /** Returns the total number of shards. */
  public int getTotalShardCount() {
    return totalShardCount;
  }

  /**
   * Encodes parity for a set of data shards.
   *
   * @param shards An array containing data shards followed by parity shards. Each shard is a byte
   *     array, and they must all be the same size.
   * @param offset The index of the first byte in each shard to encode.
   * @param byteCount The number of bytes to encode in each shard.
   */
  public void encodeParity(byte[][] shards, int offset, int byteCount) {
    // Check arguments.
    checkBuffersAndSizes(shards, offset, byteCount);

    // Build the array of output buffers.
    byte[][] outputs = new byte[parityShardCount][];
    for (int i = 0; i < parityShardCount; i++) {
      outputs[i] = shards[dataShardCount + i];
    }

    // Do the coding.
    codeSomeShards(parityRows, shards, outputs, parityShardCount, offset, byteCount);
  }

  /**
   * Returns true if the parity shards contain the right data.
   *
   * @param shards An array containing data shards followed by parity shards. Each shard is a byte
   *     array, and they must all be the same size.
   * @param firstByte The index of the first byte in each shard to check.
   * @param byteCount The number of bytes to check in each shard.
   */
  public boolean isParityCorrect(byte[][] shards, int firstByte, int byteCount) {
    // Check arguments.
    checkBuffersAndSizes(shards, firstByte, byteCount);

    // Build the array of buffers being checked.
    byte[][] toCheck = new byte[parityShardCount][];
    for (int i = 0; i < parityShardCount; i++) {
      toCheck[i] = shards[dataShardCount + i];
    }

    // Do the checking.
    return checkSomeShards(parityRows, shards, toCheck, parityShardCount, firstByte, byteCount);
  }

  /**
   * Given a list of shards, some of which contain data, fills in the ones that don't have data.
   *
   * <p>Quickly does nothing if all of the shards are present.
   *
   * <p>If any shards are missing (based on the flags in shardsPresent), the data in those shards is
   * recomputed and filled in.
   */
  public void decodeMissing(
      byte[][] shards, boolean[] shardPresent, final int offset, final int byteCount) {
    // Check arguments.
    checkBuffersAndSizes(shards, offset, byteCount);

    // Quick check: are all of the shards present?  If so, there's
    // nothing to do.
    int numberPresent = 0;
    for (int i = 0; i < totalShardCount; i++) {
      if (shardPresent[i]) {
        numberPresent += 1;
      }
    }
    if (numberPresent == totalShardCount) {
      // Cool.  All of the shards data data.  We don't
      // need to do anything.
      return;
    }

    // More complete sanity check
    if (numberPresent < dataShardCount) {
      throw new IllegalArgumentException("Not enough shards present");
    }

    // Pull out the rows of the matrix that correspond to the
    // shards that we have and build a square matrix.  This
    // matrix could be used to generate the shards that we have
    // from the original data.
    //
    // Also, pull out an array holding just the shards that
    // correspond to the rows of the submatrix.  These shards
    // will be the input to the decoding process that re-creates
    // the missing data shards.
    Matrix subMatrix = new Matrix(dataShardCount, dataShardCount);
    byte[][] subShards = new byte[dataShardCount][];
    {
      int subMatrixRow = 0;
      for (int matrixRow = 0;
          matrixRow < totalShardCount && subMatrixRow < dataShardCount;
          matrixRow++) {
        if (shardPresent[matrixRow]) {
          for (int c = 0; c < dataShardCount; c++) {
            subMatrix.set(subMatrixRow, c, matrix.get(matrixRow, c));
          }
          subShards[subMatrixRow] = shards[matrixRow];
          subMatrixRow += 1;
        }
      }
    }

    // Invert the matrix, so we can go from the encoded shards
    // back to the original data.  Then pull out the row that
    // generates the shard that we want to decode.  Note that
    // since this matrix maps back to the orginal data, it can
    // be used to create a data shard, but not a parity shard.
    Matrix dataDecodeMatrix = subMatrix.invert();

    // Re-create any data shards that were missing.
    //
    // The input to the coding is all of the shards we actually
    // have, and the output is the missing data shards.  The computation
    // is done using the special decode matrix we just built.
    byte[][] outputs = new byte[parityShardCount][];
    byte[][] matrixRows = new byte[parityShardCount][];
    int outputCount = 0;
    for (int iShard = 0; iShard < dataShardCount; iShard++) {
      if (!shardPresent[iShard]) {
        outputs[outputCount] = shards[iShard];
        matrixRows[outputCount] = dataDecodeMatrix.getRow(iShard);
        outputCount += 1;
      }
    }
    codeSomeShards(matrixRows, subShards, outputs, outputCount, offset, byteCount);

    // Now that we have all of the data shards intact, we can
    // compute any of the parity that is missing.
    //
    // The input to the coding is ALL of the data shards, including
    // any that we just calculated.  The output is whichever of the
    // data shards were missing.
    outputCount = 0;
    for (int iShard = dataShardCount; iShard < totalShardCount; iShard++) {
      if (!shardPresent[iShard]) {
        outputs[outputCount] = shards[iShard];
        matrixRows[outputCount] = parityRows[iShard - dataShardCount];
        outputCount += 1;
      }
    }
    codeSomeShards(matrixRows, shards, outputs, outputCount, offset, byteCount);
  }

  /** Checks the consistency of arguments passed to public methods. */
  private void checkBuffersAndSizes(byte[][] shards, int offset, int byteCount) {
    // The number of buffers should be equal to the number of
    // data shards plus the number of parity shards.
    if (shards.length != totalShardCount) {
      throw new IllegalArgumentException("wrong number of shards: " + shards.length);
    }

    // All of the shard buffers should be the same length.
    int shardLength = shards[0].length;
    for (int i = 1; i < shards.length; i++) {
      if (shards[i].length != shardLength) {
        throw new IllegalArgumentException("Shards are different sizes");
      }
    }

    // The offset and byteCount must be non-negative and fit in the buffers.
    if (offset < 0) {
      throw new IllegalArgumentException("offset is negative: " + offset);
    }
    if (byteCount < 0) {
      throw new IllegalArgumentException("byteCount is negative: " + byteCount);
    }
    if (shardLength < offset + byteCount) {
      throw new IllegalArgumentException("buffers to small: " + byteCount + offset);
    }
  }

  /**
   * Multiplies a subset of rows from a coding matrix by a full set of input shards to produce some
   * output shards.
   *
   * @param matrixRows The rows from the matrix to use.
   * @param inputs An array of byte arrays, each of which is one input shard. The inputs array may
   *     have extra buffers after the ones that are used. They will be ignored. The number of inputs
   *     used is determined by the length of the each matrix row.
   * @param outputs Byte arrays where the computed shards are stored. The outputs array may also
   *     have extra, unused, elements at the end. The number of outputs computed, and the number of
   *     matrix rows used, is determined by outputCount.
   * @param outputCount The number of outputs to compute.
   * @param offset The index in the inputs and output of the first byte to process.
   * @param byteCount The number of bytes to process.
   */
  private void codeSomeShards(
      final byte[][] matrixRows,
      final byte[][] inputs,
      final byte[][] outputs,
      final int outputCount,
      final int offset,
      final int byteCount) {

    // This is the inner loop.  It needs to be fast.  Be careful
    // if you change it.
    //
    // Note that dataShardCount is final in the class, so the
    // compiler can load it just once, before the loop.  Explicitly
    // adding a local variable does not make it faster.
    //
    // I have tried inlining Galois.multiply(), but it doesn't
    // make things any faster.  The JIT compiler is known to inline
    // methods, so it's probably already doing so.
    //
    // This method has been timed and compared with a C implementation.
    // This Java version is only about 10% slower than C.

    for (int iByte = offset; iByte < offset + byteCount; iByte++) {
      for (int iRow = 0; iRow < outputCount; iRow++) {
        byte[] matrixRow = matrixRows[iRow];
        int value = 0;
        for (int c = 0; c < dataShardCount; c++) {
          value ^= Galois.multiply(matrixRow[c], inputs[c][iByte]);
        }
        outputs[iRow][iByte] = (byte) value;
      }
    }
  }

  /**
   * Multiplies a subset of rows from a coding matrix by a full set of input shards to produce some
   * output shards, and checks that the the data is those shards matches what's expected.
   *
   * @param matrixRows The rows from the matrix to use.
   * @param inputs An array of byte arrays, each of which is one input shard. The inputs array may
   *     have extra buffers after the ones that are used. They will be ignored. The number of inputs
   *     used is determined by the length of the each matrix row.
   * @param toCheck Byte arrays where the computed shards are stored. The outputs array may also
   *     have extra, unused, elements at the end. The number of outputs computed, and the number of
   *     matrix rows used, is determined by outputCount.
   * @param checkCount The number of outputs to compute.
   * @param offset The index in the inputs and output of the first byte to process.
   * @param byteCount The number of bytes to process.
   */
  private boolean checkSomeShards(
      final byte[][] matrixRows,
      final byte[][] inputs,
      final byte[][] toCheck,
      final int checkCount,
      final int offset,
      final int byteCount) {

    // This is the inner loop.  It needs to be fast.  Be careful
    // if you change it.
    //
    // Note that dataShardCount is final in the class, so the
    // compiler can load it just once, before the loop.  Explicitly
    // adding a local variable does not make it faster.
    //
    // I have tried inlining Galois.multiply(), but it doesn't
    // make things any faster.  The JIT compiler is known to inline
    // methods, so it's probably already doing so.
    //
    // This method has been timed and compared with a C implementation.
    // This Java version is only about 10% slower than C.

    for (int iByte = offset; iByte < offset + byteCount; iByte++) {
      for (int iRow = 0; iRow < checkCount; iRow++) {
        byte[] matrixRow = matrixRows[iRow];
        int value = 0;
        for (int c = 0; c < dataShardCount; c++) {
          value ^= Galois.multiply(matrixRow[c], inputs[c][iByte]);
        }
        if (toCheck[iRow][iByte] != (byte) value) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Create the matrix to use for encoding, given the number of data shards and the number of total
   * shards.
   *
   * <p>The top square of the matrix is guaranteed to be an identity matrix, which means that the
   * data shards are unchanged after encoding.
   */
  private static Matrix buildMatrix(int dataShards, int totalShards) {
    // Start with a Vandermonde matrix.  This matrix would work,
    // in theory, but doesn't have the property that the data
    // shards are unchanged after encoding.
    Matrix vandermonde = vandermonde(totalShards, dataShards);

    // Multiple by the inverse of the top square of the matrix.
    // This will make the top square be the identity matrix, but
    // preserve the property that any square subset of rows  is
    // invertible.
    Matrix top = vandermonde.submatrix(0, 0, dataShards, dataShards);
    return vandermonde.times(top.invert());
  }

  /**
   * Create a Vandermonde matrix, which is guaranteed to have the property that any subset of rows
   * that forms a square matrix is invertible.
   *
   * @param rows Number of rows in the result.
   * @param cols Number of columns in the result.
   * @return A Matrix.
   */
  private static Matrix vandermonde(int rows, int cols) {
    Matrix result = new Matrix(rows, cols);
    for (int r = 0; r < rows; r++) {
      for (int c = 0; c < cols; c++) {
        result.set(r, c, Galois.exp((byte) r, c));
      }
    }
    return result;
  }
}
