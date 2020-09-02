package org.apache.ratis.failureTests;

import org.apache.ratis.MiniRaftCluster;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.MiniRaftClusterWithGrpc;
import org.apache.ratis.inst.FailureController;
import org.apache.ratis.inst.FailureInjector;
import org.apache.ratis.inst.TestConfig;
import org.apache.ratis.netty.MiniRaftClusterWithNetty;
import org.apache.ratis.protocol.RaftClientReply;
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
import org.jline.utils.Log;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.ratis.BaseTest.ONE_SECOND;
import static org.apache.ratis.inst.FailureInjector.FailureType.ARBITRARY_LINK_FAILURES;

public abstract class TestScenario {
    public static final Logger LOG = LoggerFactory.getLogger(TestScenario.class);

    protected final TestConfig conf;
    protected final MiniRaftCluster cluster;

    private ExecutorService executor; // for client requests
    protected AtomicBoolean isRun = new AtomicBoolean(false);
    protected AtomicBoolean isShutDown = new AtomicBoolean(false);

    protected AtomicInteger numSentRequests = new AtomicInteger();
    protected AtomicInteger numCompletedRequests = new AtomicInteger();

    public TestScenario(TestConfig testOptions) {
        this.conf = testOptions;
        executor = Executors.newFixedThreadPool(conf.NUM_REQUESTS);

        if(conf.CONN_ADAPTER.toUpperCase().equals("NETTY"))
            cluster = TestUtils.createNettyCluster(conf);
        else
            cluster = TestUtils.createGRPCCluster(conf);
    }

    public abstract void runTest() throws IOException, InterruptedException ;

    public abstract void recoverFailures()  ;

    public void waitForTest(long timeout, TimeUnit unit) throws InterruptedException {
        executor.shutdown();
        executor.awaitTermination(timeout, unit);
    }

    public void endTest() {
        if(!isRun.get()) LOG.info("TEST INFO: Test scenario has not completed.");
        isShutDown.set(true);

        recoverFailures();
        FailureController.logRounds(conf.OUTPUT_FOLDER_NAME, conf.OUTPUT_FILE_NAME);
        LOG.info(cluster.printServers());

        checkSpec();

        for (RaftServerImpl raftServer : cluster.iterateServerImpls()) raftServer.shutdown(false);
        cluster.shutdown();
    }


    private void checkSpec() {

        LOG.info("Number of sent client requests: " + numSentRequests.get());
        LOG.info("Number of sent completed requests: " + numCompletedRequests.get());
        LOG.info("Checking specifications.");
        RaftPeerId leader = null;
        try {
            leader = TestUtils.checkLeader(cluster, 3).getId();
        } catch (IllegalStateException e) {
            LOG.error("TestError - No leader is selected"); // TestError: Leader cannot be elected
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

        if(leader != null && !conf.FAILURE_INJECTOR_TYPE.equals(ARBITRARY_LINK_FAILURES.toString())) {
            // change leader to enforce sync if the node was isolated
            final RaftPeerId newLeader;
            try {
                newLeader = RaftTestUtil.changeLeader(cluster, leader);
                if(leader != newLeader) {
                    // allow the new leader to sync
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // Check for indices only if we have a leader and it could sync
        TestUtils.checkIndices(cluster);
    }

    // Returns the thread which waits for the reply of the request
    protected Thread simpleClientRequest(MiniRaftCluster cluster, RaftPeerId leaderId, String message, int delayMsec, int timeOutMsec) {

        Callable<Object> requestTask = new Callable<Object>() {
            public Object call() {
                RaftClient client = null;
                try {
                    Thread.sleep(delayMsec);
                    client = cluster.createClient(leaderId);
                    RaftClientReply reply = client.send(new RaftTestUtil.SimpleMessage(message)); // will timeout if less
                    Assert.assertTrue(reply.isSuccess());
                    LOG.info("Processed reply " + message);
                } catch (IOException | InterruptedException e) {
                    Log.error(e.getMessage());
                    e.printStackTrace();
                } finally {
                    if(client != null) {
                        try {
                            client.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
                return null;
            }
        };

        Thread t = new Thread( () -> {
            Future<Object> future = executor.submit(requestTask);
            try {
                Object result = future.get(timeOutMsec, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                LOG.error("TestError - Client request timed out " + message); // TestError: Client request is not processed in time out
                e.printStackTrace();
            } catch (InterruptedException | ExecutionException e) {
                // do nth
            } finally {
                future.cancel(true);
            }
        });

        t.start();
        return t;
    }

}
