package org.apache.ratis.inst;

public abstract class FailureOptions {

    public final int randomSeed;

    public FailureOptions(int randomSeed) {
        this.randomSeed = randomSeed;
    }

    abstract public void logOptions();

    abstract public String getOptionsAsStr();

}
