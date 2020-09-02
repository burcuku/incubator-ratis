package org.apache.ratis.inst;

import org.apache.ratis.inst.message.Message;
import org.apache.ratis.inst.state.ExecutionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

public class ArbitraryLinkFailureInjector extends FailureInjector {
    public static final Logger LOG = LoggerFactory.getLogger(ArbitraryLinkFailureInjector.class);

    private final ArbitraryLinkFailureOptions options;
    private final ExecutionState executionState;
    private final Random random;

    public ArbitraryLinkFailureInjector(ArbitraryLinkFailureOptions options) {
        LOG.info("Started Arbitrary Link Failure Injector");
        this.options = options;
        executionState = new ExecutionState();
        random = new Random(options.randomSeed);
    }

    public boolean isBlocked(Message m) {
        if(!ACTIVE.get()) {
            executionState.addMessage(m, false);
            return false;
        }

        boolean toDrop = !m.isLEMessage() && random.nextDouble() < options.prob;
        executionState.addMessage(m, toDrop);
        return toDrop;
    }

    public void logMessages(String folder, String traceFile) {
        executionState.logMessages(folder, traceFile);
    }

    @Override
    public void logFailures(String folder, String traceFile) {
        LOG.info("Drops messages with probability: " + options.prob);
    }

}
