package org.apache.ratis.failureTests;

import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.inst.FailureController;
import org.apache.ratis.inst.TestConfig;
import org.apache.ratis.inst.UniformSyncFailureOptions;
import org.apache.ratis.inst.failure.ExactFailureGenerator;
import org.apache.ratis.inst.failure.FailureGenerator;
import org.apache.ratis.inst.util.MathUtil;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * Failures a reinserted using certain periods of times
 * The FailureInjector isolates/deisolates messages from/to some nodes
 */
public class TestWithWhiteBoxFailures extends TestScenario {
    public static final Logger LOG = LoggerFactory.getLogger(TestWithWhiteBoxFailures.class);
    private UniformSyncFailureOptions failureOptions;
    private Random random;

    public TestWithWhiteBoxFailures(TestConfig conf) {
        super(conf);
        this.failureOptions = new UniformSyncFailureOptions(conf.RANDOM_SEED, conf.NUM_SERVERS, conf.NUM_FAILURES, conf.MAX_NUM_ROUNDS, conf.FAILURE_RECOVERY_PERIOD);
        random = new Random(failureOptions.randomSeed);
    }

    public void runTest() throws IOException, InterruptedException {
        cluster.start();
        int numPhases = failureOptions.maxNumRounds / failureOptions.recoveryPeriod;
        RaftPeer[] nodes = cluster.getPeers().toArray(new RaftPeer[0]);

        FailureGenerator failureGenerator = new ExactFailureGenerator(numPhases, conf.NUM_SERVERS, failureOptions.numFailures, random);
        List<Thread> threadsForClientReqs = new ArrayList<>();

        // wait for the leader
        RaftTestUtil.waitForLeader(cluster);
        Thread.sleep(1000);

        Map<Integer, Long> failuresOfLastPhase = new HashMap<>();
        int phase = 0;
        // for each phase/period:
        while(phase < conf.NUM_REQUESTS && !isShutDown.get()) {
            int numFailures = failureGenerator.generate();
            List<Integer> nodesToFail = MathUtil.genSubset(random, new HashSet<>(Arrays.asList(0, 1, 2)), numFailures);
            Map<Integer, Long> failures = new HashMap<>();
            // Block random set of nodes
            for(Integer node: nodesToFail) {
                long roundToFail = phase * conf.FAILURE_RECOVERY_PERIOD + random.nextInt(conf.FAILURE_RECOVERY_PERIOD);
                failures.put(node, roundToFail);
                FailureController.blockAtRound(nodes[node].getId().toString(), roundToFail); // precisely drop messages
            }
            // Unblock nodes if not failed this round
            for(Integer node: failuresOfLastPhase.keySet()) {
                // If a previously blocked node is not blocked in the beginning of the new phase, unblock the node for the prev indices
                if(!failures.containsKey(node) || (failures.get(node) % conf.FAILURE_RECOVERY_PERIOD != 0))
                    FailureController.unblockAtRound(nodes[node].getId().toString(), failuresOfLastPhase.get(node)); // precisely drop messages
            }
            failuresOfLastPhase.clear();
            failuresOfLastPhase.putAll(failures);

            // wait for election of leader
            RaftServerImpl leader = RaftTestUtil.waitForLeader(cluster);
            LOG.info("Leader: " + leader.getId());

            // send request
            Thread t = simpleClientRequest(cluster, leader.getId(),"m" + phase, 0, conf.CLIENT_REQ_TIMEOUT);
            threadsForClientReqs.add(t);

            // wait for some time for the execution
            Thread.sleep(2000);
            phase ++;
        }

        isRun.set(true);

        for (Thread threadsForClientReq : threadsForClientReqs) {
            threadsForClientReq.join();
        }
    }

    //public void runTest() throws IOException, InterruptedException {
    public void runTestDisconnectedLeader() throws IOException, InterruptedException {
        cluster.start();
        int numPhases = failureOptions.maxNumRounds / failureOptions.recoveryPeriod;
        RaftPeer[] nodes = cluster.getPeers().toArray(new RaftPeer[0]);

        FailureGenerator failureGenerator = new ExactFailureGenerator(numPhases, conf.NUM_SERVERS, failureOptions.numFailures, random);
        List<Thread> threadsForClientReqs = new ArrayList<>();

        // wait for the leader
        RaftServerImpl firstLeader = RaftTestUtil.waitForLeader(cluster);
        LOG.info("Leader: " + firstLeader.getId().toString());
        Thread.sleep(1000);

        Map<Integer, Long> failuresOfLastPhase = new HashMap<>();
        int phase = 0;
        // for each phase/period:
        while(phase < conf.NUM_REQUESTS) {
            int numFailures = failureGenerator.generate();
            //List<Integer> nodesToFail = MathUtil.genSubset(random, new HashSet<>(Arrays.asList(0, 1, 2)), numFailures);
            List<Integer> nodesToFail = new ArrayList<>();
            nodesToFail.add(Character.digit(firstLeader.getId().toString().toCharArray()[1], 10));

            Map<Integer, Long> failures = new HashMap<>();
            // Block random set of nodes
            for(Integer node: nodesToFail) {
                long roundToFail = phase * conf.FAILURE_RECOVERY_PERIOD ;
                failures.put(node, roundToFail);
                FailureController.blockAtRound(nodes[node].getId().toString(), roundToFail); // precisely drop messages
            }
            // Unblock nodes if not failed this round
            for(Integer node: failuresOfLastPhase.keySet()) {
                // If a previously blocked node is not blocked in the first round of the new phase, unblock the node for the prev indices
                if(!failures.containsKey(node) || (failures.get(node) % conf.FAILURE_RECOVERY_PERIOD != 0))
                    FailureController.unblockAtRound(nodes[node].getId().toString(), failuresOfLastPhase.get(node)); // precisely drop messages
            }
            failuresOfLastPhase.clear();
            failuresOfLastPhase.putAll(failures);

            // wait for election of leader
            RaftServerImpl leader = RaftTestUtil.waitForLeader(cluster);
            LOG.info("Leader: " + leader.getId());
            //LOG.info("First Leader: " + firstLeader.getId());

            Thread t = simpleClientRequest(cluster, leader.getId(),"m" + phase, 0, conf.CLIENT_REQ_TIMEOUT);
            threadsForClientReqs.add(t);

            // wait for some time for the execution
            Thread.sleep(2000);
            phase ++;
        }

        isRun.set(true);

        for (Thread threadsForClientReq : threadsForClientReqs) {
            threadsForClientReq.join();
        }

        FailureController.unblockAtRound(firstLeader.getId().toString(), 8);
    }

    @Override
    public void recoverFailures() {
        LOG.info("Recovering failures to allow them sync up");
        FailureController.deactivateInjector();

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

}