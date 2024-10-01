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
package org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets;

import org.apache.iotdb.tsfile.read.common.IOMonitor2;

import java.util.Arrays;

/**
 * Basic class for one wavelet keeping coefficients of the wavelet function, the scaling function,
 * the base wavelength, the forward transform method, and the reverse transform method.
 *
 * @author Christian (graetz23@gmail.com)
 * @date 10.02.2010 08:54:48
 */
public abstract class Wavelet {

  /** The name of the wavelet. */
  protected String _name;

  /** The wavelength of the base or so called mother wavelet and its matching scaling function. */
  protected int _motherWavelength;

  /** The minimal wavelength of a signal that can be transformed */
  protected int _transformWavelength;

  /** The coefficients of the mother scaling (low pass filter) for decomposition. */
  protected double[] _scalingDeCom;

  /** The coefficients of the mother wavelet (high pass filter) for decomposition. */
  protected double[] _waveletDeCom;

  /** The coefficients of the mother scaling (low pass filter) for reconstruction. */
  protected double[] _scalingReCon;

  /** The coefficients of the mother wavelet (high pass filter) for reconstruction. */
  protected double[] _waveletReCon;

  /**
   * Constructor; predefine members to default values or null!
   *
   * @date 15.02.2014 22:16:27
   * @author Christian (graetz23@gmail.com)
   */
  public Wavelet() {
    _name = null;
    _motherWavelength = 0;
    _transformWavelength = 0;
    _scalingDeCom = null;
    _waveletDeCom = null;
    _scalingReCon = null;
    _waveletReCon = null;
  } // Wavelet

  /**
   * The method builds form the scaling (low pass) coefficients for decomposition of a filter, the
   * matching coefficients for the wavelet (high pass) for decomposition, for the scaling (low pass)
   * for reconstruction, and for the wavelet (high pass) of reconstruction. This method should be
   * called in the constructor of an orthonormal filter directly after defining the orthonormal
   * coefficients of the scaling (low pass) for decomposition!
   *
   * @author Christian (graetz23@gmail.com)
   * @date 16.02.2014 13:19:27
   */
  protected void _buildOrthonormalSpace() {
    // building wavelet as orthogonal (orthonormal) space from
    // scaling coefficients (low pass filter). Have a look into
    // Alfred Haar's wavelet or the Daubechies Wavelet with 2
    // vanishing moments for understanding what is done here. ;-)
    _waveletDeCom = new double[_motherWavelength];
    for (int i = 0; i < _motherWavelength; i++) {
      if (i % 2 == 0) {
        _waveletDeCom[i] = _scalingDeCom[(_motherWavelength - 1) - i];
      } else {
        _waveletDeCom[i] = -_scalingDeCom[(_motherWavelength - 1) - i];
      }
    }
    // Copy to reconstruction filters due to orthogonality (orthonormality)!
    _scalingReCon = new double[_motherWavelength];
    _waveletReCon = new double[_motherWavelength];
    for (int i = 0; i < _motherWavelength; i++) {
      _scalingReCon[i] = _scalingDeCom[i];
      _waveletReCon[i] = _waveletDeCom[i];
    } // i
  } // _buildOrthonormalSpace

  /**
   * Returns a String keeping the name of the current Wavelet.
   *
   * @return String keeping the name of the wavelet
   * @author Christian (graetz23@gmail.com)
   * @date 17.08.2014 11:02:31
   */
  public String getName() {
    return _name;
  } // getName

  /**
   * Returns a String keeping the name of the current Wavelet. Used to override Object's toString
   * method
   *
   * @return String with the name of the wavelet
   * @author Dakota Williams
   * @date 11.06.2015 10:12:15
   */
  public String toString() {
    return getName();
  } // toString

  /**
   * Returns the wavelength of the so called mother wavelet or scaling function.
   *
   * @return the minimal wavelength for the mother wavelet
   * @author Christian (graetz23@gmail.com)
   * @date 15.02.2014 22:06:12
   */
  public int getMotherWavelength() {
    return _motherWavelength;
  } // getMotherWavelength

  /**
   * Returns the minimal necessary wavelength for a signal that can be transformed by this wavelet.
   *
   * @return integer representing minimal wavelength of the input signal that should be transformed
   *     by this wavelet.
   * @author Christian (graetz23@gmail.com)
   * @date 15.02.2014 22:08:43
   */
  public int getTransformWavelength() {
    return _transformWavelength;
  } // getTransformWavelength

  /**
   * Returns a copy of the scaling (low pass filter) coefficients of decomposition.
   *
   * @return array of length of the mother wavelet wavelength keeping the decomposition low pass
   *     filter coefficients
   * @author Christian (graetz23@gmail.com)
   * @date 15.02.2010 22:11:42
   */
  public final double[] getScalingDeComposition() {
    return Arrays.copyOf(_scalingDeCom, _scalingDeCom.length);
  } // getScalingDeComposition

  /**
   * Returns a copy of the wavelet (high pass filter) coefficients of decomposition.
   *
   * @return array of length of the mother wavelet wavelength keeping the decomposition high pass
   *     filter coefficients
   * @author Christian (graetz23@gmail.com)
   * @date 15.02.2014 22:11:25
   */
  public final double[] getWaveletDeComposition() {
    return Arrays.copyOf(_waveletDeCom, _waveletDeCom.length);
  } // getWaveletDeComposition

  /**
   * Returns a copy of the scaling (low pass filter) coefficients of reconstruction.
   *
   * @return array of length of the mother wavelet wavelength keeping the reconstruction low pass
   *     filter coefficients
   * @author Christian (graetz23@gmail.com)
   * @date 16.02.2014 10:35:11
   */
  public final double[] getScalingReConstruction() {
    return Arrays.copyOf(_scalingReCon, _scalingReCon.length);
  } // getScalingReConstruction

  /**
   * Returns a copy of the wavelet (high pass filter) coefficients of reconstruction.
   *
   * @return array of length of the mother wavelet wavelength keeping the reconstruction high pass
   *     filter coefficients
   * @author Christian (graetz23@gmail.com)
   * @date 16.02.2014 10:35:09
   */
  public final double[] getWaveletReConstruction() {
    return Arrays.copyOf(_waveletReCon, _waveletReCon.length);
  } // getWaveletReConstruction

  /**
   * Performs the forward transform for the given array from time domain to Hilbert domain and
   * returns a new array of the same size keeping coefficients of Hilbert domain and should be of
   * length 2 to the power of p -- length = 2^p where p is a positive integer.
   *
   * @param arrTime array keeping time domain coefficients
   * @param arrTimeLength is necessary, due to working only on a part of arrTime not on the full
   *     length of arrTime!
   * @return coefficients represented by frequency domain
   * @date 10.02.2010 08:18:02
   * @author Christian (graetz23@gmail.com)
   */
  public double[] forward(double[] arrTime, int arrTimeLength) {

    double[] arrHilb = new double[arrTimeLength];

    int h = arrHilb.length >> 1; // .. -> 8 -> 4 -> 2 .. shrinks in each step by half wavelength
    for (int i = 0; i < h; i++) {

      arrHilb[i] = arrHilb[i + h] = 0.; // set to zero before sum up

      for (int j = 0; j < _motherWavelength; j++) {

        IOMonitor2.DCP_D_getAllSatisfiedPageData_traversedPointNum++;

        int k = (i << 1) + j; // k = ( i * 2 ) + j;
        while (k >= arrHilb.length) {
          k -= arrHilb.length; // circulate over arrays if scaling and wavelet are are larger
        }

        arrHilb[i] +=
            arrTime[k] * _scalingDeCom[j]; // low pass filter for the energy (approximation)
        arrHilb[i + h] += arrTime[k] * _waveletDeCom[j]; // high pass filter for the details
      } // Sorting each step in patterns of: { scaling coefficients | wavelet coefficients }
    } // h = 2^(p-1) | p = { 1, 2, .., N } .. shrinks in each step by half wavelength

    return arrHilb;
  } // forward

  /**
   * Performs the reverse transform for the given array from Hilbert domain to time domain and
   * returns a new array of the same size keeping coefficients of time domain and should be of
   * length 2 to the power of p -- length = 2^p where p is a positive integer.
   *
   * @param arrHilb array keeping frequency domain coefficients
   * @param arrHilbLength is necessary, due to working only on a part of arrHilb not on the full
   *     length of arrHilb!
   * @return coefficients represented by time domain
   * @date 10.02.2010 08:19:24
   * @author Christian (graetz23@gmail.com)
   */
  public double[] reverse(double[] arrHilb, int arrHilbLength) {

    double[] arrTime = new double[arrHilbLength];
    for (int i = 0; i < arrTime.length; i++) {
      arrTime[i] = 0.; // set to zero before sum up
    }

    int h = arrTime.length >> 1; // .. -> 8 -> 4 -> 2 .. shrinks in each step by half wavelength
    for (int i = 0; i < h; i++) {

      for (int j = 0; j < _motherWavelength; j++) {

        int k = (i << 1) + j; // k = ( i * 2 ) + j;
        while (k >= arrTime.length) {
          k -= arrTime.length; // circulate over arrays if scaling and wavelet are larger
        }

        // adding up energy from low pass (approximation) and details from high pass filter
        arrTime[k] +=
            (arrHilb[i] * _scalingReCon[j])
                + (arrHilb[i + h] * _waveletReCon[j]); // looks better with brackets
      } // Reconstruction from patterns of: { scaling coefficients | wavelet coefficients }
    } // h = 2^(p-1) | p = { 1, 2, .., N } .. shrink in each step by half wavelength

    return arrTime;
  } // reverse
} // Wavelet
