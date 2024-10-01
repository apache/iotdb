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
 * Compression algorithm is adding up all magnitudes of an array, a matrix, or a space and dividing
 * this absolute value by the number of given data samples.
 *
 * @author Christian (graetz23@gmail.com)
 * @date 20.02.2014 23:56:09
 */
public class CompressorMagnitude extends Compressor {

  /**
   * Threshold is set to one, which should always guarantee a rather good compression result.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 20.02.2014 23:56:09
   */
  public CompressorMagnitude() {

    _magnitude = 0.;
    _threshold = 1.;
  } // CompressorMagnitude

  /**
   * Threshold is set a chosen; value 0 means no compression.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 20.02.2014 23:56:09
   * @param threshold has to be positive value starting at 0 - 0 means no compression.
   * @throws JWaveException
   */
  public CompressorMagnitude(double threshold) {

    super(threshold);

    _magnitude = 0.;
  } // CompressorMagnitude

  /*
   * @author Christian (graetz23@gmail.com)
   * @date 20.02.2014 23:56:09 (non-Javadoc)
   * @see jwave.compressions.Compressor#compress(double[])
   */
  @Override
  public double[] compress(double[] arrHilb) {

    _magnitude = 0.;

    int arrHilbLength = arrHilb.length;

    for (int i = 0; i < arrHilbLength; i++) _magnitude += Math.abs(arrHilb[i]);

    _magnitude /= (double) arrHilbLength;

    return compress(arrHilb, _magnitude); // generates new array
  } // compress

  /*
   * @author Christian (graetz23@gmail.com)
   * @date 20.02.2014 23:56:09 (non-Javadoc)
   * @see jwave.compressions.Compressor#compress(double[][])
   */
  @Override
  public double[][] compress(double[][] matHilb) {

    _magnitude = 0.;

    int matHilbNoOfRows = matHilb.length;
    int matHilbNoOfCols = matHilb[0].length;

    for (int i = 0; i < matHilbNoOfRows; i++)
      for (int j = 0; j < matHilbNoOfCols; j++) _magnitude += Math.abs(matHilb[i][j]);

    _magnitude /= (double) matHilbNoOfRows * (double) matHilbNoOfCols;

    return compress(matHilb, _magnitude);
  } // compress

  /*
   * @author Christian (graetz23@gmail.com)
   * @date 20.02.2014 23:56:09 (non-Javadoc)
   * @see jwave.compressions.Compressor#compress(double[][][])
   */
  @Override
  public double[][][] compress(double[][][] spcHilb) {

    _magnitude = 0.;

    int matHilbNoOfRows = spcHilb.length;
    int matHilbNoOfCols = spcHilb[0].length;
    int matHilbNoOfLvls = spcHilb[0][0].length;

    for (int i = 0; i < matHilbNoOfRows; i++)
      for (int j = 0; j < matHilbNoOfCols; j++)
        for (int k = 0; k < matHilbNoOfLvls; k++) _magnitude += Math.abs(spcHilb[i][j][k]);

    _magnitude /= (double) matHilbNoOfRows * (double) matHilbNoOfCols * (double) matHilbNoOfLvls;

    return compress(spcHilb, _magnitude);
  } // compress
} // CompressorMagnitude
