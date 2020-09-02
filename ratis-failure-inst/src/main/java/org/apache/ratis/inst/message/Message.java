package org.apache.ratis.inst.message;

import org.apache.ratis.inst.round.Round;

import java.util.concurrent.atomic.AtomicInteger;

public class Message implements Loggable {
    public final Round r;
    public final String from;
    public final String to;
    public final boolean reply;

    private AtomicInteger numExecuted = new AtomicInteger(0);

    public Message(Round r, String from, String to, boolean reply) {
        this.r = r;
        this.from = from;
        this.to = to;
        this.reply = reply;
    }

    public Integer incNumExecuted() {
        return numExecuted.incrementAndGet();
    }

    // We do not want to overwrote equals
    public boolean hasSameContent(Message other) {
        return  other.r.equals(r)
                && other.from.equals(from)
                && other.to.equals(to)
                && (other.reply == reply);
    }

    public boolean isLEMessage() {
        return r.isLERound();
    }

    public String logContent() {
        return toString();
    }

    @Override
    public String toString() {
        String replyBoolean = reply ? "T" : "F";
        return "{" +
                from + " -> " + to +
                ", r=" + r +
                ", " + replyBoolean +
                ", #:" + numExecuted.get() +
                '}';
    }

}