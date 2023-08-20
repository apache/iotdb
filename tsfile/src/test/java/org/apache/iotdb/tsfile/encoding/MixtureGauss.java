package org.apache.iotdb.tsfile.encoding;


import java.util.ArrayList;
import java.util.List;


public class MixtureGauss {
    private Component[] components;
    private DataSet data;

    public MixtureGauss(List<Double> nums, int component) {
        this.data = new DataSet(nums, component);
        this.components = new Component[component];
    }

    public List<double[]> compute(){
        //random initialization of component parameters
        for (int i = 0; i < this.data.components(); i++) {
            this.components[i] = new Component(
                    1.0 / this.components.length,
                    this.data.getMean() + ((Math.random() - 0.5) * 4),
                    this.data.getStdev() + ((Math.random() - 0.5) * 4)
            );
        }

        Double oldLog = this.logLike();
        Double newLog = oldLog - 100.0;
        do {
            oldLog = newLog;
            this.Expectation();
            this.Maximization();
            newLog = this.logLike();
//            System.out.println(newLog);
        }while (newLog!=0 && Math.abs(newLog - oldLog) > 0.00001);

        List<double[]> res = new ArrayList<>(this.components.length);
        for (Component c : this.components) {
            System.out.println(c.getWeight());
            if(c.getWeight() >= 0.00000001){
                res.add(new double[]{c.getMean(), c.getStdev(), c.getWeight()});
            }
        }
        return res;
    }

    private void Expectation() {
        for (int i = 0; i < this.data.size(); i++) {
            double[] probs = new double[this.data.components()];
            for (int j = 0; j < this.components.length; j++) {
                Component c = this.components[j];
                probs[j] = gaussian(this.data.get(i).val(), c.getMean(), c.getStdev()) * c.getWeight();
            }

            //alpha normalize and set probs
            double sum = 0.0;
            for (double p : probs)
                sum += p;
            for (int j = 0; j < probs.length; j++) {
                double normProb = probs[j]/sum;
                this.data.get(i).setProb(j, normProb);
            }
        }
    }

    private void Maximization() {
        double newMean = 0.0;
        double newStdev = 0.0;
        for (int i = 0; i < this.components.length; i++) {
            //MEAN
            for (int j = 0; j < this.data.size(); j++)
                newMean += this.data.get(j).getProb(i) * this.data.get(j).val();
            newMean /= this.data.nI(i);
            this.components[i].setMean(newMean);

            //STDEV
            for (int j = 0; j < this.data.size(); j++)
                newStdev += this.data.get(j).getProb(i) * Math.pow((this.data.get(j).val() - newMean), 2);
            newStdev /= this.data.nI(i);
            newStdev = Math.sqrt(newStdev);
            this.components[i].setStdev(newStdev);

            //WEIGHT
            this.components[i].setWeight(this.data.nI(i) / this.data.size());
        }

    }

    private Double logLike() {
        double loglike = 0.0;
        for (int i = 0; i < this.data.size(); i++) {
            double sum = 0.0;
            for (int j = 0; j < this.components.length; j++) {
                Component c = this.components[j];
                double prob = this.data.get(i).getProb(j);
                double val = this.data.get(i).val();
                double gauss = gaussian(val, c.getMean(), c.getStdev());
                if (gauss == 0) {
                    gauss = Double.MIN_NORMAL;
                }
                Double inner = Math.log(gauss)+ Math.log(c.getWeight());
                if (inner.isInfinite() || inner.isNaN()) {
                    return 0.0;
                }
                sum += prob * inner;
            }
            loglike += sum;
        }
        return loglike;
    }

    public void printStats() {
        for (Component c : this.components) {
            System.out.println("C - mean: " + c.getMean() + " stdev: " + c.getStdev() + " weight: " + c.getWeight());
        }
    }
    /*
        The following two methods courtesy of Robert Sedgewick:
        http://introcs.cs.princeton.edu/java/22library/Gaussian.java.html
        Used to calculate the PDF of a gaussian distribution with mean=mu, stddev=sigma
     */
    private double standardGaussian(double x) {
        return Math.exp(-x * x / 2) / Math.sqrt(2 * Math.PI);
    }

    private double gaussian(double x, double mu, double sigma) {
        sigma += Double.MIN_NORMAL;
        return standardGaussian((x - mu) / sigma) / sigma;
    }
}