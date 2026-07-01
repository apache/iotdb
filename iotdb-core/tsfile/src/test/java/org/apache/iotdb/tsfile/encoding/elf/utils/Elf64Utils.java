package org.apache.iotdb.tsfile.encoding.elf.utils;

public class Elf64Utils {
    // αlog_2(10) for look-up
    private final static int[] f =
        new int[] {0, 4, 7, 10, 14, 17, 20, 24, 27, 30, 34, 37, 40, 44, 47, 50, 54, 57,
            60, 64, 67};

    private final static double[] map10iP =
        new double[] {1.0, 1.0E1, 1.0E2, 1.0E3, 1.0E4, 1.0E5, 1.0E6, 1.0E7,
            1.0E8, 1.0E9, 1.0E10, 1.0E11, 1.0E12, 1.0E13, 1.0E14,
            1.0E15, 1.0E16, 1.0E17, 1.0E18, 1.0E19, 1.0E20};

    private final static double[] map10iN =
        new double[] {1.0, 1.0E-1, 1.0E-2, 1.0E-3, 1.0E-4, 1.0E-5, 1.0E-6, 1.0E-7,
            1.0E-8, 1.0E-9, 1.0E-10, 1.0E-11, 1.0E-12, 1.0E-13, 1.0E-14,
            1.0E-15, 1.0E-16, 1.0E-17, 1.0E-18, 1.0E-19, 1.0E-20};

    private final static long[] mapSPGreater1 =
        new long[] {1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000};

    private final static double[] mapSPLess1 =
        new double[] {1, 0.1, 0.01, 0.001, 0.0001, 0.00001, 0.000001, 0.0000001, 0.00000001,
            0.000000001, 0.0000000001};

    private final static double LOG_2_10 = Math.log(10) / Math.log(2);

    public static int getFAlpha(int alpha) {
        if (alpha < 0) {
            throw new IllegalArgumentException("The argument should be greater than 0");
        }
        if (alpha >= f.length) {
            return (int) Math.ceil(alpha * LOG_2_10);
        } else {
            return f[alpha];
        }
    }

    public static int[] getAlphaAndBetaStar(double v, int lastBetaStar) {
        if (v < 0) {
            v = -v;
        }
        int[] alphaAndBetaStar = new int[2];
        int[] spAnd10iNFlag = getSPAnd10iNFlag(v);
        int beta = getSignificantCount(v, spAnd10iNFlag[0], lastBetaStar);
        alphaAndBetaStar[0] = beta - spAnd10iNFlag[0] - 1;
        alphaAndBetaStar[1] = spAnd10iNFlag[1] == 1 ? 0 : beta;
        return alphaAndBetaStar;
    }

    public static double roundUp(double v, int alpha) {
        double scale = get10iP(alpha);
        if (v < 0) {
            return Math.floor(v * scale) / scale;
        } else {
            return Math.ceil(v * scale) / scale;
        }
    }

    private static int getSignificantCount(double v, int sp, int lastBetaStar) {
        int i;
        if(lastBetaStar != Integer.MAX_VALUE && lastBetaStar != 0) {
            i = Math.max(lastBetaStar - sp - 1, 1);
        } else if (lastBetaStar == Integer.MAX_VALUE) {
            i = 17 - sp - 1;
        } else if (sp >= 0) {
            i = 1;
        } else {
            i = -sp;
        }

        double temp = v * get10iP(i);
        long tempLong = (long) temp;
        while (tempLong != temp) {
            i++;
            temp = v * get10iP(i);
            tempLong = (long) temp;
        }

        // There are some bugs for those with high significand, i.e., 0.23911204406033099
        // So we should further check
        if (temp / get10iP(i) != v) {
            return 17;
        } else {
            while (i > 0 && tempLong % 10 == 0) {
                i--;
                tempLong = tempLong / 10;
            }
            return sp + i + 1;
        }
    }

    private static double get10iP(int i) {
        if (i < 0) {
            throw new IllegalArgumentException("The argument should be greater than 0");
        }
        if (i >= map10iP.length) {
            return Double.parseDouble("1.0E" + i);
        } else {
            return map10iP[i];
        }
    }

    public static double get10iN(int i) {
        if (i < 0) {
            throw new IllegalArgumentException("The argument should be greater than 0");
        }
        if (i >= map10iN.length) {
            return Double.parseDouble("1.0E-" + i);
        } else {
            return map10iN[i];
        }
    }

    public static int getSP(double v) {
        if (v >= 1) {
            int i = 0;
            while (i < mapSPGreater1.length - 1) {
                if (v < mapSPGreater1[i + 1]) {
                    return i;
                }
                i++;
            }
        } else {
            int i = 1;
            while (i < mapSPLess1.length) {
                if (v >= mapSPLess1[i]) {
                    return -i;
                }
                i++;
            }
        }
        return (int) Math.floor(Math.log10(v));
    }

    private static int[] getSPAnd10iNFlag(double v) {
        int[] spAnd10iNFlag = new int[2];
        if (v >= 1) {
            int i = 0;
            while (i < mapSPGreater1.length - 1) {
                if (v < mapSPGreater1[i + 1]) {
                    spAnd10iNFlag[0] = i;
                    return spAnd10iNFlag;
                }
                i++;
            }
        } else {
            int i = 1;
            while (i < mapSPLess1.length) {
                if (v >= mapSPLess1[i]) {
                    spAnd10iNFlag[0] = -i;
                    spAnd10iNFlag[1] = v == mapSPLess1[i] ? 1 : 0;
                    return spAnd10iNFlag;
                }
                i++;
            }
        }
        double log10v = Math.log10(v);
        spAnd10iNFlag[0] = (int) Math.floor(log10v);
        spAnd10iNFlag[1] = log10v == (long)log10v ? 1 : 0;
        return spAnd10iNFlag;
    }
}
