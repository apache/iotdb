/**
 * JWave is distributed under the MIT License (MIT); this file is part of.
 *
 * <p>Copyright (c) 2008-2024 Christian (graetz23@gmail.com)
 *
 * <p>Permission is hereby granted, free of charge, to any person obtaining a copy of this software
 * and associated documentation files (the "Software"), to deal in the Software without restriction,
 * including without limitation the rights to use, copy, modify, merge, publish, distribute,
 * sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * <p>The above copyright notice and this permission notice shall be included in all copies or
 * substantial portions of the Software.
 *
 * <p>THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING
 * BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
 * DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package org.apache.iotdb.db.query.simpiece.jwave.compressions;

/**
 * This compression takes the average of the min and absolute the max of a given array, matrix, or
 * space as thresholding value.
 *
 * @author Christian (graetz23@gmail.com)
 * @date 14.05.2015 17:57:19
 */
public class CompressorPeaksAverage extends Compressor {

  protected double _peakMinimum;

  protected double _peakMaximum;

  /**
   * Compression by average of peak min & max.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 14.05.2015 17:57:19
   */
  public CompressorPeaksAverage() {

    _peakMinimum = 0.;
    _peakMaximum = 0.;
    _magnitude = 0.;
  } // CompressorPeaksAverage

  /**
   * Compression by average of peak min & max.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 14.05.2015 17:57:19
   * @param threshold
   * @throws JWaveException
   */
  public CompressorPeaksAverage(double threshold) {

    super(threshold);

    _peakMinimum = 0.;
    _peakMaximum = 0.;
    _magnitude = 0.;
  } // CompressorPeaksAverage

  /**
   * Calculating the average by maximal distance between minimal and maximal absolute value.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 14.05.2015 17:57:19 (non-Javadoc)
   * @see jwave.compressions.Compressor#compress(double[])
   */
  @Override
  public double[] compress(double[] arrHilb) {

    for (double val : arrHilb) {

      double absVal = Math.abs(val);

      if (absVal <= _peakMinimum) _peakMinimum = absVal;

      if (absVal >= _peakMaximum) _peakMaximum = absVal;
    } // loop over all entries

    _magnitude = .5 * (_peakMaximum - _peakMinimum);

    return compress(arrHilb, _magnitude);
  } // compress

  /**
   * Calculating the average by maximal distance between minimal and maximal absolute value.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 14.05.2015 17:57:19 (non-Javadoc)
   * @see jwave.compressions.Compressor#compress(double[][])
   */
  @Override
  public double[][] compress(double[][] matHilb) {

    int matHilbNoOfRows = matHilb.length;
    int matHilbNoOfCols = matHilb[0].length;

    for (int i = 0; i < matHilbNoOfRows; i++) {
      for (int j = 0; j < matHilbNoOfCols; j++) {

        double absVal = Math.abs(matHilb[i][j]);

        if (absVal <= _peakMinimum) _peakMinimum = absVal;

        if (absVal >= _peakMaximum) _peakMaximum = absVal;
      } // j
    } // i - loop over all entries

    _magnitude = .5 * (_peakMaximum - _peakMinimum);

    return compress(matHilb, _magnitude);
  } // compress

  /**
   * Calculating the average by maximal distance between minimal and maximal absolute value.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 14.05.2015 17:57:19 (non-Javadoc)
   * @see jwave.compressions.Compressor#compress(double[][][])
   */
  @Override
  public double[][][] compress(double[][][] spcHilb) {

    int matHilbNoOfRows = spcHilb.length;
    int matHilbNoOfCols = spcHilb[0].length;
    int matHilbNoOfLvls = spcHilb[0][0].length;

    for (int i = 0; i < matHilbNoOfRows; i++) {
      for (int j = 0; j < matHilbNoOfCols; j++) {
        for (int k = 0; k < matHilbNoOfLvls; k++) {

          double absVal = Math.abs(spcHilb[i][j][k]);

          if (absVal <= _peakMinimum) _peakMinimum = absVal;

          if (absVal >= _peakMaximum) _peakMaximum = absVal;
        } // k
      } // j
    } // i - loop over all entries

    _magnitude = .5 * (_peakMaximum - _peakMinimum);

    return compress(spcHilb, _magnitude);
  } // compress
} // class
