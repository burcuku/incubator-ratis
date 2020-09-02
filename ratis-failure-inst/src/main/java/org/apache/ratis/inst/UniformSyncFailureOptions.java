package org.apache.ratis.inst;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UniformSyncFailureOptions extends FailureOptions {
    public static final Logger LOG = LoggerFactory.getLogger(UniformSyncFailureOptions.class);
    enum InjectionTYpe {
        INTERNAL_READS_PHASES, // reads the contents of messages
        EXTERNAL_ABSTRACT_PHASES // does not read messages, isolates/deisolates nodes
    }

    public final int numServers;
    public final int numFailures;
    public final int maxNumRounds;
    public final int recoveryPeriod;
    // calculates numPhases as maxNumRounds / faultRecoveryPeriod

    public UniformSyncFailureOptions(int randomSeed, int numServers, int numFailures, int maxNumRounds, int faultRecoveryPeriod) {
        super(randomSeed);
        this.numServers = numServers;
        this.numFailures = numFailures;
        this.maxNumRounds = maxNumRounds;
        this.recoveryPeriod = faultRecoveryPeriod;
    }

    public void logOptions() {
        LOG.info(getOptionsAsStr());
    }

    @Override
    public String getOptionsAsStr() {
        String s = "";
        s = s.concat("Failure Injector Options: " + "\n");
        s = s.concat("Random seed: " + randomSeed + "\n");
        s = s.concat("Number of servers: " + numServers + "\n");
        s = s.concat("Number of failures: " + numFailures + "\n");
        s = s.concat("Max number of rounds: " + maxNumRounds + "\n");
        s = s.concat("Fault recovery period: " + recoveryPeriod + "\n");
        return s;
    }
}
