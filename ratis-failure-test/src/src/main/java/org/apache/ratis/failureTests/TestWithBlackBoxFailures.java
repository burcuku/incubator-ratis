package org.apache.ratis.failureTests;

import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.inst.FailureController;
import org.apache.ratis.inst.FailureInjector;
import org.apache.ratis.inst.TestConfig;
import org.apache.ratis.inst.UniformSyncFailureOptions;
import org.apache.ratis.inst.failure.ExactFailureGenerator;
import org.apache.ratis.inst.failure.FailureGenerator;
import org.apache.ratis.inst.util.MathUtil;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.impl.BlockRequestHandlingInjection;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.IntStream;

/**
 * Failures a reinserted using certain periods of times
 * The FailureInjector isolates/deisolates messages from/to some nodes
 */
public class TestWithBlackBoxFailures extends TestScenario {
    public static final Logger LOG = LoggerFactory.getLogger(TestWithBlackBoxFailures.class);

    // This class creates and maintains its own failures by viewing the system as a black box
    private UniformSyncFailureOptions failureOptions;
    private Random random;

    private final int numPhases;
    private final int isolationType;

    private RaftPeer[] nodes;
    private Set<Integer> nodesAlive;

    public TestWithBlackBoxFailures(TestConfig conf) {
        super(conf);
        if (conf.FAILURE_INJECTOR_TYPE.equals(FailureInjector.FailureType.UNIFORM_SYNC_FAILURES_BLACKBOX_API.toString())) {
            isolationType = 1;
        } else if(conf.FAILURE_INJECTOR_TYPE.equals(FailureInjector.FailureType.UNIFORM_SYNC_FAILURES_BLACKBOX_RESTART.toString())) {
            isolationType = 2;
        } else {
            isolationType = 0;
            LOG.error("Unknown failure injection type");
            System.exit(-1);
        }

        this.failureOptions = new UniformSyncFailureOptions(conf.RANDOM_SEED, conf.NUM_SERVERS, conf.NUM_FAILURES, conf.MAX_NUM_ROUNDS, conf.FAILURE_RECOVERY_PERIOD);
        random = new Random(failureOptions.randomSeed);

        numPhases = failureOptions.maxNumRounds / failureOptions.recoveryPeriod;
    }

    // returns the executor for client requests
    public void runTest() throws IOException, InterruptedException {
        cluster.start();
        nodes = cluster.getPeers().toArray(new RaftPeer[0]);
        List<Integer> allNodes = new ArrayList<>();
        IntStream.range(0, nodes.length).forEach(allNodes::add);

        FailureGenerator failureGenerator = new ExactFailureGenerator(numPhases, conf.NUM_SERVERS, failureOptions.numFailures, random);
        List<Thread> threadsForClientReqs = new ArrayList<>();
        int sentRequests = 0;

        int phase = 1;

        nodesAlive = new HashSet<>();
        Set<Integer> nodesFailed = new HashSet<>();
        Set<Integer> nodesFailedInPrevRound = new HashSet<>(); //from last round

        // for each phase/period:
        while(sentRequests < conf.NUM_REQUESTS && !isShutDown.get()) {
            cluster.printServers();
            // generate failures
            int numFailures = failureGenerator.generate();
            int numFailures1 = (numFailures > 0) ? random.nextInt(numFailures) : 0;
            int numFailures2 = numFailures - numFailures1;

            // part 1 - isolate a subset of nodes and elect a leader (steady state)
            nodesAlive.addAll(allNodes);
            nodesFailed.clear();
            if(numFailures1 > 0) {
                List<Integer> nodesToFail1 = MathUtil.genSubset(random, new HashSet<>(Arrays.asList(0, 1, 2)), numFailures1); //todo parametrize!
                LOG.info("Phase " + phase + " A  failures: " + Arrays.toString(nodesToFail1.toArray()));
                nodesToFail1.forEach(n -> {
                    if (nodesFailedInPrevRound.contains(n)) {
                        nodesAlive.remove(n);
                        nodesFailed.add(n);
                    } else {
                        boolean isIsolated = isolate(nodes[n].getId());
                        if(isIsolated) {
                            nodesAlive.remove(n); // remove from alive nodes only if isolated (may not be isolated if shut down hook is running)
                            nodesFailed.add(n);
                        }
                    }
                });

            } else {
                LOG.info("Phase " + phase + " A  No failures.");
            }

            nodesAlive.forEach(n -> {
                if (nodesFailedInPrevRound.contains(n)) {
                    deIsolate(nodes[n].getId());
                }

            });

            // continue if cluster has no quorums
            if(nodesAlive.size() < ((nodes.length / 2) + 1)) {
                nodesFailedInPrevRound.addAll(nodesFailed);
                LOG.info("Not enough number of alive nodes, will move to the next phase.");
                Thread.sleep(2000);
                LOG.info("Moved to the next phase.");
                continue;
            }

            // wait for election of leader
            RaftServerImpl leader = RaftTestUtil.waitForLeader(cluster);
            LOG.info("Leader: " + leader.getId());
            cluster.printServers();

            int timeoutMsec = conf.CLIENT_REQ_TIMEOUT;
            if(isolationType == 2) timeoutMsec += failureGenerator.getRemainingFaultBudget() * 2000; // each restart takes around 2 sec

            Thread t = simpleClientRequest(cluster, leader.getId(),"m" + (++sentRequests), 0, timeoutMsec);
            threadsForClientReqs.add(t);

            // part 2 - isolate again a subset of alive nodes
            if(numFailures2 > 0) {
                List<Integer> nodesToFail2 = MathUtil.genSubset(random, nodesAlive, numFailures2);
                LOG.info("Phase " + phase + " B  failures: " + Arrays.toString(nodesToFail2.toArray()));
                nodesToFail2.forEach(n -> {
                    boolean isIsolated = isolate(nodes[n].getId());
                    if(isIsolated) {
                        nodesAlive.remove(n);
                        nodesFailed.add(n);
                    }
                });
            } else {
                LOG.info("Phase " + phase + " B  No failures.");
            }

            // wait for some time for the execution
            Thread.sleep(2000);
            nodesFailedInPrevRound.clear();
            nodesFailedInPrevRound.addAll(nodesFailed);
            phase ++;
        }

        isRun.set(true);

        for (Thread threadsForClientReq : threadsForClientReqs) {
            threadsForClientReq.join();
        }
    }

    // The methods for isolate/deisolate are synchronized because they can be called by the main test thread and the shut down hook concurrently
    private synchronized boolean isolate(RaftPeerId id) {
        if(isShutDown.get()) return false; // do not isolate new nodes while the test is being ended by the shut down hook

        if(isolationType == 1) {
            LOG.info(" --  Isolating by API: " + id.toString());
            BlockRequestHandlingInjection.getInstance().blockReplier(id.toString());
            cluster.setBlockRequestsFrom(id.toString(), true);
        } else if(isolationType == 2) {
            LOG.info(" --  Isolating by killing server: " + id.toString());
            cluster.killServer(id);
        } else {
            return false;
        }

        return true;
    }

    private synchronized void deIsolate(RaftPeerId id) {
        if(isolationType == 1) {
            LOG.info(" --  Deisolating by API: " + id.toString());
            BlockRequestHandlingInjection.getInstance().unblockReplier(id.toString());
            cluster.setBlockRequestsFrom(id.toString(), false);
        } else if(isolationType == 2) {
            LOG.info(" --  Deisolating by restarting server: " + id.toString());
            try {
                cluster.restartServer(id, false);
                Thread.sleep(2000);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void recoverFailures() {
        LOG.info("Recovering failures to allow them sync up");
        FailureController.deactivateInjector();

        if (isolationType == 2) { // restart
            List<Integer> allNodes = new ArrayList<>();
            IntStream.range(0, nodes.length).forEach(allNodes::add);

            // deisolate all nodes
            allNodes.forEach(n -> {
                if(!nodesAlive.contains(n)) {
                    deIsolate(nodes[n].getId());
                }
            });
        } else { // API
            List<Integer> allNodes = new ArrayList<>();
            IntStream.range(0, nodes.length).forEach(allNodes::add);

            // deisolate all nodes
            allNodes.forEach(n -> deIsolate(nodes[n].getId()));
        }

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }



}