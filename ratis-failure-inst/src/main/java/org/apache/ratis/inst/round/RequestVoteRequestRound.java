package org.apache.ratis.inst.round;

import org.apache.ratis.proto.RaftProtos;

public class RequestVoteRequestRound extends Round {

    public RequestVoteRequestRound(RaftProtos.RequestVoteRequestProto proto) {
        this(REQUESTVOTE_REQUEST,
                proto.getCandidateTerm(),
                proto.getCandidateLastEntry().getIndex());
    }

    private RequestVoteRequestRound(String messageType, long termId, long index) {
        this.messageType = messageType;
        this.termId = termId;
        this.index = index;
    }

    @Override
    public boolean equals(Object o) {
        if(!(o instanceof RequestVoteRequestRound)) return false;

        RequestVoteRequestRound other = (RequestVoteRequestRound) o;

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
