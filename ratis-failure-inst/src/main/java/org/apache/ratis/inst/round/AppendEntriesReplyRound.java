package org.apache.ratis.inst.round;

import org.apache.ratis.proto.RaftProtos;

public class AppendEntriesReplyRound extends Round {
    protected boolean isHB; //is heart beat reply

    public AppendEntriesReplyRound(RaftProtos.AppendEntriesReplyProto proto) {
        this(proto.getIsHearbeat(),
                proto.getServerReply().getSuccess(),
                proto.getTerm(),
                proto.getFollowerCommit());
    }

    private AppendEntriesReplyRound(boolean isHB, boolean reply, long termId, long index) {
        this.isHB = isHB;
        this.termId = termId;
        this.index = index;

        if(isHB)
            this.messageType = APPENDENTRIES_REPLY_HEARTBEAT;
        else
            this.messageType = APPENDENTRIES_REPLY;
    }

    public boolean getIsHB() {
        return isHB;
    }

    @Override
    public boolean equals(Object o) {
        if(!(o instanceof AppendEntriesReplyRound)) return false;

        AppendEntriesReplyRound other = (AppendEntriesReplyRound) o;

        return  other.messageType.equals(((AppendEntriesReplyRound) o).messageType)
                && other.termId == termId
                && other.index == index
                && (other.isHB == isHB);
    }

    @Override
    public String toString() {
        return "{" + messageType + '\'' +
                ", termId=" + termId +
                ", index=" + index +
                '}';
    }
}