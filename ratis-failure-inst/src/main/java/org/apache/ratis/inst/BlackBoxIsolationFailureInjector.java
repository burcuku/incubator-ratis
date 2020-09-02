package org.apache.ratis.inst;

import org.apache.ratis.inst.message.Message;
import org.apache.ratis.inst.util.FileLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/** This injector is deprecated and is not used in the final version of tests
 *  (This injector uses instrumentation to process block/unblock calls inside tests)
 *  Please refer to TestWithBlackBoxFailures and TestWithWhiteBoxFailures testing classes.
 */

/**
 * Provides methods for isolating/deisolating nodes
 */
public class BlackBoxIsolationFailureInjector extends FailureInjector implements BlackBoxFailureInjector {
    public static final Logger LOG = LoggerFactory.getLogger(BlackBoxIsolationFailureInjector.class);

    List<String> peers = Arrays.asList("s0", "s1", "s2"); //todo input
    Map<String, Boolean> blockedFrom = new HashMap<>();
    Map<String, Boolean> blockedTo = new HashMap<>();

    public BlackBoxIsolationFailureInjector() {
        super();
        LOG.info("Initiating " + TestConfig.getInstance().FAILURE_INJECTOR_TYPE);
        peers.forEach(p -> {
            blockedFrom.put(p, false);
            blockedTo.put(p, false);
        });
    }

    private void blockFrom(String nodeId) {
        blockedFrom.put(nodeId, true);
    }

    private void blockTo(String nodeId) {
        blockedTo.put(nodeId, true);
    }

    private void unblockFrom(String nodeId) {
        blockedFrom.put(nodeId, false);
    }

    private void unblockTo(String nodeId) {
        blockedTo.put(nodeId, false);
    }

    public boolean isBlockedFrom(String nodeId) {
        return blockedFrom.get(nodeId);
    }

    public boolean isBlockedTo(String nodeId) {
        return blockedTo.get(nodeId);
    }

    @Override
    public boolean isBlocked(Message message) {
        if(!ACTIVE.get()) {
            executionState.addMessage(message, false);
            return false;
        }

        boolean blocked =  blockedFrom.get(message.from) || blockedTo.get(message.to);
        executionState.addMessage(message, blocked);
        return blocked;
    }

    @Override
    public void block(String nodeId) {
        blockFrom(nodeId);
        blockTo(nodeId);
    }

    @Override
    public void unblock(String nodeId) {
        unblockFrom(nodeId);
        unblockTo(nodeId);
    }
}
