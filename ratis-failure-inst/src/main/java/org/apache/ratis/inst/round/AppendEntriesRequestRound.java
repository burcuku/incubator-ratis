package org.apache.ratis.inst.round;

import org.apache.ratis.proto.RaftProtos;

import java.util.List;

public class AppendEntriesRequestRound extends Round {

    private List<RaftProtos.LogEntryProto> entries;

    public AppendEntriesRequestRound(RaftProtos.AppendEntriesRequestProto proto) {
        this(proto.getLeaderTerm(),
                proto.getLeaderCommit(),
                proto.getEntriesList());
    }

    private AppendEntriesRequestRound(long termId, long index, List<RaftProtos.LogEntryProto> entries) {
        this.termId = termId;
        this.index = index;
        this.entries = entries;

        if(entries.isEmpty())
            this.messageType = APPENDENTRIES_REQUEST_HEARTBEAT;
        else if (entries.get(0).hasConfigurationEntry())
            this.messageType = APPENDENTRIES_REQUEST_CONFIG;
        else if (entries.get(0).hasMetadataEntry())
            this.messageType = APPENDENTRIES_REQUEST_METADATA;
        else if (entries.get(0).hasStateMachineLogEntry())
            this.messageType = APPENDENTRIES_REQUEST_STATEMACHINE;
        else  this.messageType = APPENDENTRIES_REQUEST;
    }

    public List<RaftProtos.LogEntryProto> getEntries() {
        return entries;
    }

    @Override
    public boolean equals(Object o) {
        if(!(o instanceof AppendEntriesRequestRound)) return false;

        AppendEntriesRequestRound other = (AppendEntriesRequestRound) o;

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
