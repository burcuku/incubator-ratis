package org.apache.ratis.inst;

public interface WhiteBoxFailureInjector {

    // the methods are called for blocking messages after/before certain rounds

    // block messages from given round to the end of phase
    void blockAtRound(String nodeId, long roundId);

    // unblock messages pertaining to a phase earlier or equal to the the phase of roundId
    void unblockAtRound(String nodeId, long roundId);

}