package org.apache.ratis.inst;

import org.apache.ratis.inst.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class WhiteBoxIsolationFailureInjector extends FailureInjector implements WhiteBoxFailureInjector {
    public static final Logger LOG = LoggerFactory.getLogger(WhiteBoxIsolationFailureInjector.class);

    List<String> peers = Arrays.asList("s0", "s1", "s2"); //todo input
    Map<String, List<Long>> blocked = new HashMap<>();

    //TestConfig conf = TestConfig.getInstance();
    final UniformSyncFailureOptions failureOptions;


    public WhiteBoxIsolationFailureInjector(UniformSyncFailureOptions failureOptions) {
        super();
        this.failureOptions = failureOptions;
        LOG.info("Initiating WhiteBoxIsolationFailureInjector");
        peers.forEach(p -> {
            blocked.put(p, new ArrayList<>());
        });
    }

    @Override
    public boolean isBlocked(Message message) {
        if(!ACTIVE.get()) {
            executionState.addMessage(message, false);
            return false;
        }

        boolean isBlocked =  blocked.get(message.from).contains(message.r.getIndex()) || blocked.get(message.to).contains(message.r.getIndex());
        executionState.addMessage(message, isBlocked);
        return isBlocked;
    }

    // Block the messages of the phase of roundId - from round roundId until the end of the phase
    @Override
    public void blockAtRound(String nodeId, long roundId) {
        List<Long> rounds  = blocked.get(nodeId);

        for(long i = roundId; i < ((roundId / failureOptions.recoveryPeriod) + 1) * failureOptions.recoveryPeriod; i++) {
            LOG.info("Blocked node " + nodeId + " round: " + i);
            rounds.add(i);
        }

        blocked.put(nodeId, rounds);
    }

    // Unblock messages earlier than or equal to the phase of roundId
    @Override
    public void unblockAtRound(String nodeId, long roundId) {
        List<Long> rounds  = blocked.get(nodeId);
        for(long i = 0; i < ((roundId / failureOptions.recoveryPeriod) + 1) * failureOptions.recoveryPeriod; i++) {
            if(rounds.contains(i)) LOG.info("Unblocked node " + nodeId + " round: " + i);
            rounds.remove(i);
        }
        blocked.put(nodeId, rounds);
    }
}

