package org.apache.ratis.inst;

public interface BlackBoxFailureInjector {

    // the methods are called after certain timeouts to abstract rounds
    void block(String nodeId);

    void unblock(String nodeId);

}
