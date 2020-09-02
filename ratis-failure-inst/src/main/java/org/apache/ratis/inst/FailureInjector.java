package org.apache.ratis.inst;

import org.apache.ratis.inst.message.Message;
import org.apache.ratis.inst.state.ExecutionState;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class FailureInjector {

    public enum FailureType {
        ARBITRARY_LINK_FAILURES,
        UNIFORM_SYNC_FAILURES_WHITEBOX,
        UNIFORM_SYNC_FAILURES_BLACKBOX_API,
        UNIFORM_SYNC_FAILURES_BLACKBOX_RESTART,
    }

    protected ExecutionState executionState;

    // the injector can be turned off to allow messages to allow the nodes to synchronize themselves
    protected AtomicBoolean ACTIVE = new AtomicBoolean(true);

    public FailureInjector() {
        executionState = new ExecutionState();
    }

    abstract public boolean isBlocked(Message message);

    public void logMessages(String folder, String traceFile) {
        executionState.logMessages(folder, traceFile);
    }

    public void logFailures(String folder, String traceFile) {
        executionState.logFailures(folder, traceFile);
    }

    public void deactivate() {
        ACTIVE.set(false);
    }

    public void activate() {
        ACTIVE.set(true);
    }

    public boolean isActive() {
        return ACTIVE.get();
    }
}
