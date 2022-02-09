/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package apache.iotdb.quality.validity;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class ValidityComputing {

    private int cnt = 0;//数据点总数
    private int violations = 0;
    private int errorCnt = 0;//违背约束的数据点个数
    private List<Double> valueWindow = new ArrayList<>();//除去特殊值的时间序列
    private List<Long> timeWindow = new ArrayList<>();
    private double speedAVG = 0;
    private double speedSTD = 0;
    private List<Boolean> lastRepair = new ArrayList<>();
    private List<Boolean> firstRepair = new ArrayList<>();
    private List<Integer> DP = new ArrayList<>();
    private List<Integer> reverseDP = new ArrayList<>();
    private double xMax = Double.MAX_VALUE;
    private double xMin = -Double.MAX_VALUE + 1;
    private boolean repairSelfLast = true;
    private boolean repairSelfFirst = true;
    private int indexLastRepaired = -1;

    public ValidityComputing() throws Exception {
    }

    public ValidityComputing(String filename) throws Exception {
        Scanner sc = new Scanner(new File(filename));
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        sc.useDelimiter("\\s*(,|\\r|\\n)\\s*");//设置分隔符，以逗号或回车分隔，前后可以有若干个空白符
        sc.nextLine();
        while (sc.hasNext()) {
            cnt++;
            long t = format.parse(sc.next()).getTime();
            double v = sc.nextDouble();
            if (Double.isFinite(v)) {
                updateAll(t, v);
            }
        }
    }


    public void updateAll(long time, double value) {
        cnt++;
        int index = timeWindow.size();
        timeWindow.add(time);
        valueWindow.add(value);
        if (index <= 1) {
            return;
        }
        double timeLastInterval = timeWindow.get(index) - timeWindow.get(index - 1);
        if (timeLastInterval != 0) {
            double speedNow = (valueWindow.get(index) - valueWindow.get(index - 1)) / timeLastInterval;
            updateAVGSTD(speedNow);
        }
    }

    public void updateDPAll(boolean usePreSpeed, double sMin, double sMax) {

        double smax = this.speedAVG + 3 * this.speedSTD;
        double smin = this.speedAVG - 3 * this.speedSTD;
        if (Math.abs(smax) > Math.abs(smin)) {
            smin = -(this.speedAVG + 3 * this.speedSTD);
        } else {
            smax = -(this.speedAVG - 3 * this.speedSTD);
        }
        if (usePreSpeed) {
            smin = sMin;
            smax = sMax;
        }
        //    double smax = 1;
        //    double smin = -1;
        firstRepair.add(false);
        DP.add(0);
        for (int index = 1; index < timeWindow.size(); index++) {
            Long time = timeWindow.get(index);
            Double value = valueWindow.get(index);
            int dp = -1;
            boolean find = false;
            if (value < xMin || value > xMax) {
                dp = 100000000;
                firstRepair.add(true);
                DP.add(dp);
                System.out.println(value);
                System.out.println(xMin);
                System.out.println(xMax);
                System.out.println("out of Range");
                continue;
            }
            for (int i = 0; i < index; i++) {
                if (valueWindow.get(i) < xMin || valueWindow.get(i) > xMax) {
                    continue;
                }
                if (time - timeWindow.get(i) == 0) {
                    continue;
                }
                double speedNow = (value - valueWindow.get(i)) / (time - timeWindow.get(i));
                if (speedNow <= smax && speedNow >= smin) {
                    find = true;
                    if (dp == -1) {
                        dp = DP.get(i) + index - i - 1;
                        if (firstRepair.get(i)) {
                            if (firstRepair.size() >= index + 1) {
                                firstRepair.set(index, true);
                            } else {
                                firstRepair.add(true);
                            }
                        } else {
                            if (firstRepair.size() >= index + 1) {
                                firstRepair.set(index, false);
                            } else {
                                firstRepair.add(false);
                            }
                        }
                    } else {
                        if (DP.get(i) + index - i - 1 < dp) {
                            dp = DP.get(i) + index - i - 1;
                            if (firstRepair.get(i)) {
                                if (firstRepair.size() >= index + 1) {
                                    firstRepair.set(index, true);
                                } else {
                                    firstRepair.add(true);
                                }
                            } else {
                                if (firstRepair.size() >= index + 1) {
                                    firstRepair.set(index, false);
                                } else {
                                    firstRepair.add(false);
                                }
                            }
                        }
                    }
                }
            }
            if (!find) {
                dp = index;
                firstRepair.add(true);
            }
            DP.add(dp);
        }
    }


    public void updateReverseDPAll(boolean usePreSpeed, double sMin, double sMax) {

        double smax = this.speedAVG + 3 * this.speedSTD;
        double smin = this.speedAVG - 3 * this.speedSTD;
        if (Math.abs(smax) > Math.abs(smin)) {
            smin = -(this.speedAVG + 3 * this.speedSTD);
        } else {
            smax = -(this.speedAVG - 3 * this.speedSTD);
        }
        if (usePreSpeed) {
            smin = sMin;
            smax = sMax;
        }
        //    double smax = 1;
        //    double smin = -1;

        lastRepair.add(false);
        reverseDP.add(0);

        int Length = this.timeWindow.size();

        for (int j = Length - 2; j >= 0; j--) {
            Long time = timeWindow.get(j);
            Double value = valueWindow.get(j);
            int dp = -1;
            boolean find = false;
            if (value < xMin || value > xMax) {
                dp = 100000000;
                lastRepair.add(true);
                reverseDP.add(dp);
                continue;
            }
            for (int i = Length - 1; i > j; i--) {
                if (valueWindow.get(i) < xMin || valueWindow.get(i) > xMax) {
                    continue;
                }
                int index = Length - i - 1;
                if (time - timeWindow.get(i) == 0) {
                    continue;
                }
                double speedNow = (value - valueWindow.get(i)) / (time - timeWindow.get(i));
                if (speedNow <= smax && speedNow >= smin) {
                    find = true;
                    if (dp == -1) {
                        dp = reverseDP.get(index) + i - j - 1;
                        if (lastRepair.get(index)) {
                            if (lastRepair.size() >= Length - j) {
                                lastRepair.set(Length - j - 1, true);
                            } else {
                                lastRepair.add(true);
                            }
                        } else {
                            if (lastRepair.size() >= Length - j) {
                                lastRepair.set(Length - j - 1, false);
                            } else {
                                lastRepair.add(false);
                            }
                        }
                    } else {
                        if (reverseDP.get(index) + i - j - 1 < dp) {
                            dp = reverseDP.get(index) + i - j - 1;
                            if (lastRepair.get(index)) {
                                if (lastRepair.size() >= Length - j) {
                                    lastRepair.set(Length - j - 1, true);
                                } else {
                                    lastRepair.add(true);
                                }
                            } else {
                                if (lastRepair.size() >= Length) {
                                    lastRepair.set(Length - j - 1, false);
                                } else {
                                    lastRepair.add(false);
                                }
                            }
                        }
                    }
                }
            }
            if (!find) {
                dp = Length - 1;
                lastRepair.add(true);
            }
            reverseDP.add(dp);
        }
        int validityErrorsTemp = this.cnt;
        for (int m = 0; m < Length; m++) {
            if (validityErrorsTemp > DP.get(m) + reverseDP.get(Length - 1 - m)) {
                validityErrorsTemp = DP.get(m) + reverseDP.get(Length - 1 - m);
                indexLastRepaired = m;
            }
        }
        errorCnt = validityErrorsTemp;
        if (this.indexLastRepaired == -1) {
            this.repairSelfFirst = false;
            this.repairSelfLast = false;
            return;
        }
        if (this.firstRepair.get(this.indexLastRepaired)) {
            this.repairSelfFirst = false;
        }
        if (this.lastRepair.get(this.indexLastRepaired)) {
            this.repairSelfLast = false;
        }
    }


    public void updateAVGSTD(double speedNow) {
        double variance = Math.pow(this.speedSTD, 2);
        this.speedSTD =
                Math.sqrt(
                        (cnt - 1) / Math.pow(cnt, 2) * Math.pow(speedNow - this.speedAVG, 2)
                                + (double) (cnt - 1) / cnt * variance);
        this.speedAVG = this.speedAVG + (speedNow - this.speedAVG) / cnt;
    }


    public static void main(String[] args) throws Exception {
        ValidityComputing tsq = new ValidityComputing("temp.csv");
        tsq.updateDPAll(false,1,-1);
        tsq.updateReverseDPAll(false, 1,-1);
        System.out.println(tsq.getValidity());
    }

    public void computeViolation(boolean usePreSpeed, double sMin, double sMax) {

        double smax = this.speedAVG + 3 * this.speedSTD;
        double smin = this.speedAVG - 3 * this.speedSTD;
        if (Math.abs(smax) > Math.abs(smin)) {
            smin = -(this.speedAVG + 3 * this.speedSTD);
        } else {
            smax = -(this.speedAVG - 3 * this.speedSTD);
        }
        if (usePreSpeed) {
            smin = sMin;
            smax = sMax;
        }
        for (int index = 1; index < timeWindow.size(); index++) {
            Long time = timeWindow.get(index);
            Double value = valueWindow.get(index);
            if (value < xMin || value > xMax) {
                violations += 1;
                continue;
            }
            boolean hasCompute = false;
            for (int i = 0; i < index; i++) {
                if (hasCompute) {
                    break;
                }
                if (valueWindow.get(i) < xMin || valueWindow.get(i) > xMax) {
                    violations += 1;
                    hasCompute = true;
                    continue;
                }
                if (time - timeWindow.get(i) == 0) {
                    violations += 1;
                    hasCompute = true;
                    continue;
                }
                double speedNow = (value - valueWindow.get(i)) / (time - timeWindow.get(i));
                if (speedNow > smax || speedNow < smin) {
                    violations += 1;
                    hasCompute = true;
                }
            }
        }
    }

    /**
     * 返回时间序列的有效性
     *
     * @return 有效性
     */
    public double getValidity() {
        System.out.println(errorCnt);
        return 1 - (double)errorCnt / cnt;
    }

    public double getViolation() {
        System.out.println(violations);
        System.out.println(cnt);
        return 1 - (double)violations / cnt;
    }
}
