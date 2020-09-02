package org.apache.ratis.inst;

import org.apache.ratis.inst.message.Message;
import org.apache.ratis.inst.round.*;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.proto.netty.NettyProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.ratis.inst.FailureInjector.FailureType.*;
import static org.apache.ratis.inst.FailureInjector.FailureType.UNIFORM_SYNC_FAILURES_BLACKBOX_RESTART;

public class FailureController {
    public static final Logger LOG = LoggerFactory.getLogger(FailureController.class);

    private static FailureInjector fi;
    private static boolean initiated = false; // Failure Controller has no effect if it is not initiated
    private static boolean printBlocked = false; //for debugging purposes

    public static void initiate(TestConfig config) {
        if(config.FAILURE_INJECTOR_TYPE.equals(ARBITRARY_LINK_FAILURES.toString())) {
            ArbitraryLinkFailureOptions failureOptions = new ArbitraryLinkFailureOptions(config.RANDOM_SEED, config.DROP_WITH_PROB);
            fi = new ArbitraryLinkFailureInjector((ArbitraryLinkFailureOptions) failureOptions);
            fi.logFailures(config.OUTPUT_FOLDER_NAME, config.OUTPUT_FILE_NAME);
        } else if(config.FAILURE_INJECTOR_TYPE.equals(UNIFORM_SYNC_FAILURES_WHITEBOX.toString())) {
            UniformSyncFailureOptions failureOptions = new UniformSyncFailureOptions(config.RANDOM_SEED, config.NUM_SERVERS, config.NUM_FAILURES, config.MAX_NUM_ROUNDS, config.FAILURE_RECOVERY_PERIOD);
            fi = new WhiteBoxIsolationFailureInjector(failureOptions);
        } else if(config.FAILURE_INJECTOR_TYPE.equals(UNIFORM_SYNC_FAILURES_BLACKBOX_RESTART.toString())) {
            //fi = new BlackBoxFailureInjector(); // failures will be inserted inside test - using cluster, peer methods
            return;
        } else if(config.FAILURE_INJECTOR_TYPE.equals(UNIFORM_SYNC_FAILURES_BLACKBOX_API.toString())) {
            //fi = new BlackBoxFailureInjector(); // failures will be inserted inside test - using API
            return;
        }

        initiated = true;
    }

    public static boolean isBlocked(NettyProtos.RaftNettyServerRequestProto proto) {
        if(!initiated) return false;

        if(proto.hasRequestVoteRequest()) {
            RaftProtos.RaftRpcRequestProto req = proto.getRequestVoteRequest().getServerRequest();
            Message m = new Message(new RequestVoteRequestRound(proto.getRequestVoteRequest()), req.getRequestorId().toStringUtf8(), req.getReplyId().toStringUtf8(), false);
            boolean blocked = fi.isBlocked(m);
            if(printBlocked && blocked) System.out.println("  -- Dropped: " + m);
            return blocked;
        } else if (proto.hasAppendEntriesRequest()) {
            RaftProtos.RaftRpcRequestProto req = proto.getAppendEntriesRequest().getServerRequest();
            Message m = new Message(new AppendEntriesRequestRound(proto.getAppendEntriesRequest()), req.getRequestorId().toStringUtf8(), req.getReplyId().toStringUtf8(), false);
            boolean blocked = fi.isBlocked(m);
            if(printBlocked && blocked) System.out.println("  -- Dropped: " + m);
            return blocked;
        }

        return false;
    }

    public static boolean isBlocked(NettyProtos.RaftNettyServerReplyProto proto) {
        if(!initiated) return false;

        if(proto.hasRequestVoteReply()) {
            RaftProtos.RaftRpcReplyProto rep = proto.getRequestVoteReply().getServerReply();
            Message m = new Message(new RequestVoteReplyRound(proto.getRequestVoteReply()), rep.getReplyId().toStringUtf8(), rep.getRequestorId().toStringUtf8(), rep.getSuccess());
            boolean blocked = fi.isBlocked(m);
            if(printBlocked && blocked) System.out.println("  -- Dropped: " + m);
            return blocked;
        } else if (proto.hasAppendEntriesReply()) {
            RaftProtos.RaftRpcReplyProto rep = proto.getAppendEntriesReply().getServerReply();
            Message m = new Message(new AppendEntriesReplyRound(proto.getAppendEntriesReply()), rep.getReplyId().toStringUtf8(), rep.getRequestorId().toStringUtf8(), rep.getSuccess());
            boolean blocked = fi.isBlocked(m);
            if(printBlocked && blocked) System.out.println("  -- Dropped: " + m);
            return blocked;
        }
        return false;
    }

    public static boolean isBlocked(RaftProtos.RequestVoteRequestProto proto) {
        if(!initiated) return false;

        RaftProtos.RaftRpcRequestProto req =  proto.getServerRequest();
        Message m = new Message(new RequestVoteRequestRound(proto), req.getRequestorId().toStringUtf8(), req.getReplyId().toStringUtf8(), false);
        boolean blocked = fi.isBlocked(m);
        if(printBlocked && blocked) System.out.println("  -- Dropped: " + m);
        return blocked;
    }

    public static boolean isBlocked(RaftProtos.AppendEntriesRequestProto proto) {
        if(!initiated) return false;

        RaftProtos.RaftRpcRequestProto req =  proto.getServerRequest();
        Message m = new Message(new AppendEntriesRequestRound(proto), req.getRequestorId().toStringUtf8(), req.getReplyId().toStringUtf8(), false);
        boolean blocked = fi.isBlocked(m);
        if(printBlocked && blocked) System.out.println("  -- Dropped: " + m);
        return blocked;
    }

    public static boolean isBlocked(RaftProtos.RequestVoteReplyProto proto) {
        if(!initiated) return false;

        RaftProtos.RaftRpcReplyProto rep = proto.getServerReply();
        Message m = new Message(new RequestVoteReplyRound(proto), rep.getReplyId().toStringUtf8(), rep.getRequestorId().toStringUtf8(), rep.getSuccess());
        boolean blocked = fi.isBlocked(m);
        if(printBlocked && blocked) System.out.println("  -- Dropped: " + m);
        return blocked;
    }

    public static boolean isBlocked(RaftProtos.AppendEntriesReplyProto proto) {
        if(!initiated) return false;

        RaftProtos.RaftRpcReplyProto rep = proto.getServerReply();
        Message m = new Message(new AppendEntriesReplyRound(proto), rep.getReplyId().toStringUtf8(), rep.getRequestorId().toStringUtf8(), rep.getSuccess());
        boolean blocked = fi.isBlocked(m);
        if(printBlocked && blocked) System.out.println("  -- Dropped: " + m);
        return blocked;
    }

    public static void deactivateInjector() {
        if(!initiated) return;

        LOG.info("Deactivated failure injector");
        fi.deactivate();
    }

    public static void activateInjector() {
        if(!initiated) return;
        LOG.info("Reactivated failure injector");
        fi.activate();
    }

    public static void logRounds(String folder, String traceFile) {
        if(!initiated) return;
        fi.logMessages(folder, traceFile);
    }

    public static void logFailures(String folder, String traceFile) {
        if(!initiated) return;
        fi.logFailures(folder, traceFile);
    }


    // block/unblock nodes - used for black box failures:
    // (they are not used as BlackBoxIsolationFailureInjector is deprecated)
    public static void block(String nodeId) {
        if(!initiated || !(fi instanceof BlackBoxFailureInjector)) return;

        ((BlackBoxFailureInjector) fi).block(nodeId);
    }

    public static void unblock(String nodeId) {
        if(!initiated || !(fi instanceof BlackBoxFailureInjector)) return;

        ((BlackBoxFailureInjector) fi).unblock(nodeId);
    }

    // used for white box failures:
    public static void blockAtRound(String nodeId, long roundId) {
        if(!initiated || !(fi instanceof WhiteBoxFailureInjector)) return;

        ((WhiteBoxFailureInjector) fi).blockAtRound(nodeId, roundId);
    }

    public static void unblockAtRound(String nodeId, long roundId) {
        if(!initiated || !(fi instanceof WhiteBoxFailureInjector)) return;

        ((WhiteBoxFailureInjector) fi).unblockAtRound(nodeId, roundId);
    }

}
