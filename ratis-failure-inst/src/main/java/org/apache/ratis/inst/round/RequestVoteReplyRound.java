package org.apache.ratis.inst.round;

import org.apache.ratis.proto.RaftProtos;

public class RequestVoteReplyRound extends Round {

    private boolean reply;

    public RequestVoteReplyRound(RaftProtos.RequestVoteReplyProto proto) {
        this(proto.getTerm(), -1, proto.getServerReply().getSuccess());
    }

    private RequestVoteReplyRound(long termId, long index, boolean reply) {
        this.messageType = REQUESTVOTE_REPLY;
        this.termId = termId;
        this.index = index;
        this.reply = reply;
    }

    public boolean getReply() {
        return reply;
    }

    @Override
    public boolean equals(Object o) {
        if(!(o instanceof RequestVoteReplyRound)) return false;

        RequestVoteReplyRound other = (RequestVoteReplyRound) o;

        return  other.messageType.equals(messageType)
                && other.termId == termId
                && other.index == index;
    }

    @Override
    public String toString() {
        return "{" + messageType + '\'' +
                ", termId=" + termId +
                ", index=" + index +
                '}';
    }
}
