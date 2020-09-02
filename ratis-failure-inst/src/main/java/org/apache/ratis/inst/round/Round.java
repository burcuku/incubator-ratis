package org.apache.ratis.inst.round;

public abstract class Round {
    protected String messageType;
    protected long termId;
    protected long index;

    public static final String REQUESTVOTE_REPLY = "VOTE-REP";
    public static final String APPENDENTRIES_REPLY = "AE-REP";
    public static final String APPENDENTRIES_REPLY_HEARTBEAT = "AE-REP-HB";

    public static final String REQUESTVOTE_REQUEST = "VOTE-REQ";
    public static final String APPENDENTRIES_REQUEST = "AE-REQ";
    public static final String APPENDENTRIES_REQUEST_STATEMACHINE = "AE-REQ-SM";
    public static final String APPENDENTRIES_REQUEST_METADATA = "AE-REQ-MD";
    public static final String APPENDENTRIES_REQUEST_HEARTBEAT = "AE-REQ-HB";
    public static final String APPENDENTRIES_REQUEST_CONFIG = "AE-REQ-CONF";

    public String getMessageType() {
        return messageType;
    }

    public long getTermId() {
        return termId;
    }

    public long getIndex() {
        return index;
    }

    public boolean isRequestRound() {
        return messageType.equals(REQUESTVOTE_REQUEST) || messageType.equals(APPENDENTRIES_REQUEST)
                || messageType.equals(APPENDENTRIES_REQUEST_STATEMACHINE) || messageType.equals(APPENDENTRIES_REQUEST_METADATA)
                || messageType.equals(APPENDENTRIES_REQUEST_HEARTBEAT) || messageType.equals(APPENDENTRIES_REQUEST_CONFIG) ;
    }

    public boolean isReplyRound() {
        return messageType.equals(REQUESTVOTE_REQUEST) || messageType.equals(APPENDENTRIES_REPLY )
                || messageType.equals(APPENDENTRIES_REPLY_HEARTBEAT );
    }

    public boolean isLERound() {
        return messageType.equals(REQUESTVOTE_REQUEST) || messageType.equals(REQUESTVOTE_REPLY);
    }

    @Override
    public String toString() {
        return "[ " + messageType + /*" reply: " +*/ " " + " t: " + termId + "  i: " + index /*+ "  direction: "  + d*/ + " ]";
    }
}
