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

import org.apache.iotdb.db.query.simpiece.jwave.exceptions.JWaveException;
import org.apache.iotdb.db.query.simpiece.jwave.exceptions.JWaveFailure;

/**
 * Some how this class is doing the same as the technical counterpart is doing - compressing data
 * that is transformed to Hilbert space by different methods.
 *
 * @author Christian (graetz23@gmail.com)
 * @date 20.02.2014 23:41:35
 */
public abstract class Compressor {

  /**
   * A threshold that is used in several compression methods.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 20.02.2014 23:44:26
   */
  protected double _threshold = 1.;

  /**
   * The calculated magnitude by algorithm of derived class.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 14.05.2015 18:36:17
   */
  protected double _magnitude = 0.;

  /**
   * @author Christian (graetz23@gmail.com)
   * @date 20.02.2014 23:41:35
   */
  public Compressor() {

    _magnitude = 0.;
    _threshold = 1.;
  } // Compressor

  public Compressor(double threshold) {

    _magnitude = 0.;

    try {

      if (threshold <= 0.)
        throw new JWaveFailure("Compressor - given threshold should be larger than zero!");
    } catch (JWaveException e) {

      e.showMessage();
      System.out.println("Compressor - setting threshold to default value: " + 1.);
      threshold = 1.;
    }

    _threshold = threshold;
  } // Compressor

  /**
   * Compresses by comparing the magnitude value by the set compression factor, the threshold, and
   * the sets all lower values to zero.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 14.05.2015 18:06:52
   * @param arr
   * @param magnitude
   * @return
   */
  protected double[] compress(double[] arr, double magnitude) {

    int arrLength = arr.length;

    double[] arrComp = new double[arrLength];

    for (int i = 0; i < arrLength; i++)
      if (Math.abs(arr[i]) >= magnitude * _threshold) arrComp[i] = arr[i];
      else arrComp[i] = 0.; // compression be setting to zero

    return arrComp;
  } // compress

  /**
   * Compresses by comparing the magnitude value by the set compression factor, the threshold, and
   * the sets all lower values to zero.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 14.05.2015 18:17:31
   * @param mat
   * @param magnitude
   * @return
   */
  protected double[][] compress(double[][] mat, double magnitude) {

    int matHilbNoOfRows = mat.length;
    int matHilbNoOfCols = mat[0].length;

    double[][] matComp = new double[matHilbNoOfRows][matHilbNoOfCols];

    for (int i = 0; i < matHilbNoOfRows; i++)
      for (int j = 0; j < matHilbNoOfCols; j++)
        if (Math.abs(mat[i][j]) >= magnitude * _threshold) matComp[i][j] = mat[i][j];
        else matComp[i][j] = 0.;

    return matComp;
  } // compress

  /**
   * Compresses by comparing the magnitude value by the set compression factor, the threshold, and
   * the sets all lower values to zero.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 14.05.2015 18:18:44
   * @param spc
   * @param magnitude
   * @return
   */
  protected double[][][] compress(double[][][] spc, double magnitude) {

    int matHilbNoOfRows = spc.length;
    int matHilbNoOfCols = spc[0].length;
    int matHilbNoOfLvls = spc[0][0].length;

    double[][][] spcComp = new double[matHilbNoOfRows][matHilbNoOfCols][matHilbNoOfLvls];

    for (int i = 0; i < matHilbNoOfRows; i++)
      for (int j = 0; j < matHilbNoOfCols; j++)
        for (int k = 0; k < matHilbNoOfLvls; k++)
          if (Math.abs(spc[i][j][k]) >= magnitude * _threshold) spcComp[i][j][k] = spc[i][j][k];
          else spcComp[i][j][k] = 0.;

    return spcComp;
  } // compress

  /**
   * Calculate the compression rate for a given array; means the percentage of the number of zeros
   * kept by the array.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 06.01.2016 23:47:06
   * @param arr the input of array keeping values and zeros
   * @return the compression rate as a percentage: [0 .. 100] %
   */
  public double calcCompressionRate(double[] arr) {

    double compressionRate = 0.;
    int noOfZeros = 0;
    int length = arr.length;
    for (int i = 0; i < length; i++) {
      if (arr[i] == 0.) noOfZeros++;
    } // i
    if (noOfZeros != 0) compressionRate = (double) noOfZeros / (double) length * 100.;
    else compressionRate = (double) noOfZeros;
    return compressionRate;
  } // calcCompressionRate

  /**
   * Getter for _threshold member.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 14.05.2015 18:38:33
   * @return value of threshold member
   */
  public double getThreshold() {

    return _threshold;
  } // getThreshold

  /**
   * Getter for calculated _magnitude member.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 14.05.2015 18:39:32
   * @return value of magnitude member
   */
  public double getMagnitude() {

    return _magnitude;
  } // getMagnitude

  /**
   * Interface for arrays for driving the different compression methods.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 20.02.2014 23:48:06
   * @param arrHilb
   * @return
   */
  public abstract double[] compress(double[] arrHilb);

  /**
   * Interface for matrices for driving the different compression methods.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 20.02.2014 23:54:11
   * @param matHilb
   * @return
   */
  public abstract double[][] compress(double[][] matHilb);

  /**
   * Interface for spaces for driving the different compression methods.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 20.02.2014 23:54:52
   * @param spcHilb
   * @return
   */
  public abstract double[][][] compress(double[][][] spcHilb);
} // Compressor
