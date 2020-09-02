package org.apache.ratis.failureTests;

import org.apache.ratis.MiniRaftCluster;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.MiniRaftClusterWithGrpc;
import org.apache.ratis.inst.TestConfig;
import org.apache.ratis.netty.MiniRaftClusterWithNetty;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.statemachine.SimpleStateMachine4Testing;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.*;

import static org.apache.ratis.BaseTest.ONE_SECOND;

public class TestUtils {
    public static final Logger LOG = LoggerFactory.getLogger(TestUtils.class);

    public static MiniRaftCluster createNettyCluster(TestConfig conf) {
        RaftProperties prop = createProperties(conf);
        return MiniRaftClusterWithNetty.FACTORY.newCluster(conf.NUM_SERVERS, prop);
    }

    public static  MiniRaftCluster createGRPCCluster(TestConfig conf) {
        RaftProperties prop = createProperties(conf);
        return MiniRaftClusterWithGrpc.FACTORY.newCluster(conf.NUM_SERVERS, prop);
    }

    public static  RaftProperties createProperties(TestConfig conf) {
        final RaftProperties prop = new RaftProperties();
        prop.setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY, SimpleStateMachine4Testing.class, StateMachine.class);

        RaftServerConfigKeys.Log.Appender.setInstallSnapshotEnabled(prop, false);
        RaftServerConfigKeys.Snapshot.setAutoTriggerThreshold(prop, conf.SNAPSHOT_TRIGGER_THRESHOLD);
        RaftServerConfigKeys.Snapshot.setAutoTriggerEnabled(prop, true);
        //RaftServerConfigKeys.Log.setPurgeGap(prop, PURGE_GAP);
        RaftServerConfigKeys.Log.setSegmentSizeMax(prop, SizeInBytes.valueOf(1024)); // 1k segment*/
        RaftServerConfigKeys.Notification.setNoLeaderTimeout(prop, TimeDuration.valueOf(conf.NO_LEADER_TIMEOUT, TimeUnit.SECONDS));
        RaftServerConfigKeys.Rpc.setRequestTimeout(prop, TimeDuration.valueOf(2, TimeUnit.SECONDS));
        return prop;
    }

    public static RaftServerImpl checkLeader(MiniRaftCluster cluster, int numLeaderAttempts) throws InterruptedException {
        LOG.info("SPEC - Checking whether a new leader can be elected indices of servers");
        final RaftServerImpl leader = JavaUtils.attemptRepeatedly(
                () -> Optional.ofNullable(cluster.getLeader())
                        .orElseThrow(() -> new IllegalStateException("No leader is selected")),
                numLeaderAttempts, ONE_SECOND, "getLeaderId", LOG);

        return leader;
    }

    public static boolean checkIndices(MiniRaftCluster cluster) {
        LOG.info("SPEC - Checking log indices of servers");
        List<RaftPeer> peers = cluster.getPeers();
        Map<RaftPeer, Long> indices = new HashMap<>();
        for (RaftPeer rp: peers) {
            long nextIndex = cluster.getRaftServerImpl(rp.getId()).getState().getLog().getNextIndex();
            LOG.info("Raft peer: " + rp.getId() + "  Next index: " + nextIndex);
            indices.put(rp, nextIndex);
        }
        for (int i = 0; i < peers.size(); i++) {
            for(int j = i+1; j < peers.size(); j++) {
                long index1 = cluster.getRaftServerImpl(peers.get(i).getId()).getState().getLog().getNextIndex();
                long index2 = cluster.getRaftServerImpl(peers.get(j).getId()).getState().getLog().getNextIndex();
                Assert.assertEquals("Server indices are synchronized", index1, index2);
            }
        }
        return true;
    }

    public static boolean checkIndicesWithLeader(MiniRaftCluster cluster, RaftPeerId leader) {
        LOG.info("SPEC - Checking log indices of servers");
        RaftLog leaderLog = cluster.getRaftServerImpl(leader).getState().getLog();
        LOG.info("Leader next index: " + leaderLog.getNextIndex());
        for (RaftPeer rp : cluster.getPeers()) {
            if (rp.getId() != leader) {
                RaftLog log = cluster.getRaftServerImpl(rp.getId()).getState().getLog();
                LOG.info("Follower " + rp.getId() + " next index: " + log.getNextIndex());
                Assert.assertEquals("Server indices are synchronized", leaderLog.getNextIndex(), log.getNextIndex());
            }
        }

        return true;
    }

    public static Thread setTimer(String timerInfo, int timeoutMillis) { // is this needed now?
        Thread timerThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(timeoutMillis);
                    LOG.info("Timeout is reached. Timer stops the execution. Info: " + timerInfo);
                } catch (InterruptedException e) {
                    LOG.info("Timer thread interrupted - main task finished.");
                    //e.printStackTrace();
                }
                System.exit(-1); // shut down cluster, etc
            }
        });
        timerThread.start();
        return timerThread;
    }

}
