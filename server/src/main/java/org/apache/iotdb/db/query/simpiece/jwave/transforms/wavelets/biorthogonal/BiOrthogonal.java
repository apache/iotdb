/**
 * Base class for BiOrthogonal wavelet keeping construction and different forward and reverse
 * methods.
 *
 * @author Christian (graetz23@gmail.com)
 * @date 28.03.2015 18:42:08
 *     <p>BiOrthogonal.java
 */
package org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.biorthogonal;

import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.Wavelet;
import org.apache.iotdb.tsfile.read.common.IOMonitor2;

/**
 * Base class for BiOrthogonal wavelets.
 *
 * @author Christian (graetz23@gmail.com)
 * @date 28.03.2015 18:42:08
 */
public class BiOrthogonal extends Wavelet {

  /**
   * no jobs done here ..
   *
   * @author Christian (graetz23@gmail.com)
   * @date 28.03.2015 18:42:08
   */
  public BiOrthogonal() {} // BiOrthogonal

  /**
   * The method builds form the scaling (low pass) coefficients for decomposition and wavelet (high
   * pass) coefficients for decomposition of a filter, the matching coefficients for the scaling
   * (low pass) for reconstruction, and for the wavelet (high pass) of reconstruction. This method
   * should be called in the constructor of an biorthogonal (biorthonormal) filter directly after
   * defining the orthonormal coefficients of the scaling (low pass) and wavelet (high pass) for
   * decomposition!
   *
   * @author Christian (graetz23@gmail.com)
   * @date 16.02.2014 17:04:44
   */
  protected void _buildBiOrthonormalSpace() {
    // building wavelet and scaling function for reconstruction
    // as orthogonal (orthonormal) spaces from scaling and wavelet
    // of decomposition. ;-)
    _scalingReCon = new double[_motherWavelength];
    _waveletReCon = new double[_motherWavelength];
    for (int i = 0; i < _motherWavelength; i++) {
      //      if( i % 2 == 0 ) {
      //        _scalingReCon[ i ] = _waveletDeCom[ ( _motherWavelength - 1 ) - i ];
      //        _waveletReCon[ i ] = _scalingDeCom[ ( _motherWavelength - 1 ) - i ];
      //      } else {
      //        _scalingReCon[ i ] = -_waveletDeCom[ ( _motherWavelength - 1 ) - i ];
      //        _waveletReCon[ i ] = -_scalingDeCom[ ( _motherWavelength - 1 ) - i ];
      //      } // if
      if (i % 2 == 0) {
        _scalingReCon[i] = -_waveletDeCom[i];
        _waveletReCon[i] = -_scalingDeCom[i];
      } else {
        _scalingReCon[i] = _waveletDeCom[i];
        _waveletReCon[i] = _scalingDeCom[i];
      } // if
    } // i
  } // _buildBiOrthonormalSpace

  /**
   * Wavelet forward transform algorithm adapted to biorthogonal wavelets.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 28.03.2015 18:44:24 (non-Javadoc)
   * @see Wavelet#forward(double[], int)
   */
  @Override
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
   * Wavelet reverse transform algorithm adapted to biorthogonal wavelets.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 28.03.2015 18:44:24 (non-Javadoc)
   * @see Wavelet#reverse(double[], int)
   */
  @Override
  public double[] reverse(double[] arrHilb, int arrHilbLength) {

    double[] arrTime = new double[arrHilbLength];
    for (int i = 0; i < arrTime.length; i++) {
      arrTime[i] = 0.;
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
} // class
