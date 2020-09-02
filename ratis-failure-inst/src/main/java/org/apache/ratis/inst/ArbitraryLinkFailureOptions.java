package org.apache.ratis.inst;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ArbitraryLinkFailureOptions extends FailureOptions {
    public static final Logger LOG = LoggerFactory.getLogger(UniformSyncFailureOptions.class);
    double prob;

    public ArbitraryLinkFailureOptions(int randomSeed, double probabilityOfDrop) {
        super(randomSeed);
        this.prob = probabilityOfDrop;
    }

    public void logOptions() {
        LOG.info(getOptionsAsStr());
    }

    @Override
    public String getOptionsAsStr() {
        String s = "";
        s = s.concat("Failure Injector Options: \n");
        s = s.concat("Random seed: " + randomSeed + "\n");
        s = s.concat("Drops a message with probability: " + prob + "\n");
        return s;
    }
}
