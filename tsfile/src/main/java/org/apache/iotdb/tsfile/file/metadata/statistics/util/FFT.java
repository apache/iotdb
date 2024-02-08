package org.apache.iotdb.tsfile.file.metadata.statistics.util;

import java.util.Arrays;

public class FFT {

  public static Complex[] fft(double[] a, int length) {
    Complex[] b = new Complex[a.length];
    for (int i = 0; i < a.length; i++) b[i] = new Complex(a[i], 0);
    return fft(b, length);
  }

  // FFT with padding or croping
  public static Complex[] fft(Complex[] a, int length) {
    if (length < a.length) return fft(Arrays.copyOf(a, length));
    if (length > a.length) {
      Complex[] b = new Complex[length];
      for (int i = 0; i < a.length; i++) b[i] = a[i];
      for (int i = a.length; i < length; i++) b[i] = new Complex(0, 0);
      return fft(b);
    }
    return fft(a);
  }

  private static Complex[] fft(Complex[] a) {
    int n = a.length;

    // Base case
    if (n == 1) return new Complex[] {a[0]};

    // Split the array into even and odd parts
    Complex[] even = new Complex[n / 2];
    Complex[] odd = new Complex[n / 2];
    for (int i = 0; i < n / 2; i++) {
      even[i] = a[i * 2];
      odd[i] = a[i * 2 + 1];
    }

    // Recursive FFT
    Complex[] evenResult = fft(even);
    Complex[] oddResult = fft(odd);

    // Combine
    Complex[] result = new Complex[n];
    for (int i = 0; i < n / 2; i++) {
      double angle = -2 * i * Math.PI / n;
      Complex t = new Complex(Math.cos(angle), Math.sin(angle)).multiply(oddResult[i]);
      result[i] = evenResult[i].add(t);
      result[i + n / 2] = evenResult[i].subtract(t);
    }

    return result;
  }

  public static double[] ifft(Complex[] a) {
    int n = a.length;

    // Take conjugate of the input
    for (int i = 0; i < n; i++) {
      a[i] = a[i].conjugate();
    }

    // Compute FFT using conjugated input
    Complex[] tmp = fft(a);
    double[] result = new double[n];

    // Take conjugate of the output and scale
    for (int i = 0; i < n; i++) {
      result[i] = tmp[i].conjugate().scale(1.0 / n).getReal();
    }

    return result;
  }

  public static void main(String[] args) {
    Complex[] a = {new Complex(1, 0), new Complex(2, 0), new Complex(3, 0), new Complex(4, 0)};

    System.out.println("Input: " + Arrays.toString(a));

    Complex[] fftResult = fft(new double[] {1, 2, 3, 4}, 4);
    System.out.println("FFT: " + Arrays.toString(fftResult));

    double[] ifftResult = ifft(fftResult);
    System.out.println("IFFT: " + Arrays.toString(ifftResult));
  }
}
