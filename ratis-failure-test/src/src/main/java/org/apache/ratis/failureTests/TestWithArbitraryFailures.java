package org.apache.ratis.failureTests;

import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.inst.FailureController;
import org.apache.ratis.inst.TestConfig;
import org.apache.ratis.inst.UniformSyncFailureOptions;
import org.apache.ratis.inst.failure.ExactFailureGenerator;
import org.apache.ratis.inst.failure.FailureGenerator;
import org.apache.ratis.inst.util.MathUtil;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

/**
 * Failures a reinserted using certain periods of times
 * The FailureInjector isolates/deisolates messages from/to some nodes
 */
public class TestWithArbitraryFailures extends TestScenario {
    public static final Logger LOG = LoggerFactory.getLogger(TestWithArbitraryFailures.class);
    // Failures are arbitrarily inserted by ArbitraryFailureInjector, initiated in the main test class
    private ExecutorService executor; // for client requests

    public TestWithArbitraryFailures(TestConfig conf) {
        super(conf);
        executor = Executors.newFixedThreadPool(conf.NUM_REQUESTS);
    }

    // returns the executor for client requests
    public void runTest() throws IOException, InterruptedException {
        cluster.start();
        RaftServerImpl leader = RaftTestUtil.waitForLeader(cluster);
        LOG.info("Leader elected: " + leader.getId().toString());
        sendClientRequests(conf.NUM_REQUESTS); // should redirect to new leader inc ase of disconnection
        isRun.set(true);
    }

    private void sendClientRequests(int numRequests) throws InterruptedException {
        // divide numRequests into 4 batches with phase duration of execution time in between
        Thread threads[] = new Thread[numRequests];
        for (int i = 1; i <= numRequests; i++) {
            threads[i - 1] = simpleClientRequest(i, "m" + i, ((i - 1)) * 2000); // wait between sets of requests for about a period of time
        }
        for (int i = 1; i <= numRequests; i++) {
            threads[i - 1].join();
        }
    }

    protected Thread simpleClientRequest(int requestId, String message, int delayMsec) {
        // todo optimize
        Callable<Object> requestTask = new Callable<Object>() {
            public Object call() {
                RaftClient client = null;
                try {
                    Thread.sleep(delayMsec);
                    final RaftPeerId leaderId = cluster.getLeader().getId();
                    client = cluster.createClient(leaderId);
                    numSentRequests.getAndIncrement();
                    RaftClientReply reply = client.send(new RaftTestUtil.SimpleMessage(message)); // will timeout if less
                    numCompletedRequests.getAndIncrement();
                } catch (IOException | InterruptedException e) {
                    LOG.error(e.getMessage());
                    //e.printStackTrace();
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
                Object result = future.get(conf.CLIENT_REQ_TIMEOUT + delayMsec, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                LOG.error("TestError - Client request " + requestId + " timed out with " + conf.CLIENT_REQ_TIMEOUT + " msecs."); // TestError: Client request is not processed in time out
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
