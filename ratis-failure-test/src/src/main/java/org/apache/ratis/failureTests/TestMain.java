package org.apache.ratis.failureTests;

import org.apache.ratis.inst.*;
import org.apache.ratis.inst.util.FileLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static org.apache.ratis.inst.FailureInjector.FailureType.*;
import static org.apache.ratis.inst.FailureInjector.FailureType.UNIFORM_SYNC_FAILURES_BLACKBOX_RESTART;

public class TestMain {
    public static final Logger LOG = LoggerFactory.getLogger(TestMain.class);

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        TestConfig config = TestConfig.initialize("test.conf", args);
        FailureController.initiate(config);
        TestScenario testScenario;
        if(config.FAILURE_INJECTOR_TYPE.equals(ARBITRARY_LINK_FAILURES.toString())) {
            LOG.info("Running test: " + config.FAILURE_INJECTOR_TYPE);
            testScenario = new TestWithArbitraryFailures(config);
        } else if(config.FAILURE_INJECTOR_TYPE.equals(UNIFORM_SYNC_FAILURES_WHITEBOX.toString())) {
            LOG.info("Running test: " + config.FAILURE_INJECTOR_TYPE);
            testScenario = new TestWithWhiteBoxFailures(config);
        } else if(config.FAILURE_INJECTOR_TYPE.equals(UNIFORM_SYNC_FAILURES_BLACKBOX_API.toString())
                || config.FAILURE_INJECTOR_TYPE.equals(UNIFORM_SYNC_FAILURES_BLACKBOX_RESTART.toString())) {
            LOG.info("Running test: " + config.FAILURE_INJECTOR_TYPE);
            testScenario = new TestWithBlackBoxFailures(config);
        } else {
            LOG.error("Unknown failure injector type");
            return;
        }

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                testScenario.endTest();
                long stopTime = System.currentTimeMillis();
                String message = "--- Test " + TestConfig.getInstance().TEST_ID + " has completed in " + (stopTime - startTime)/1000 + " seconds.";
                FileLogger.logToFile(config.OUTPUT_FOLDER_NAME, config.OUTPUT_FILE_NAME, message, true);
                LOG.info(message);
            }
        });

        boolean failuresByRestarts = config.FAILURE_INJECTOR_TYPE.equals(UNIFORM_SYNC_FAILURES_BLACKBOX_RESTART.toString());
        int execTimeOfTestInMsec = failuresByRestarts ? (config.PHASE_LENGTH * config.NUM_REQUESTS) + (config.NUM_FAILURES * 2000) + 4000 : (config.PHASE_LENGTH * config.NUM_REQUESTS) + 4000;

        Thread timerThread = TestUtils.setTimer("Timer for the test", execTimeOfTestInMsec);
        testScenario.runTest();
        testScenario.waitForTest(execTimeOfTestInMsec, TimeUnit.MILLISECONDS); // will check for spec after this
        LOG.info("Returning from wait for test");
        timerThread.interrupt();
        //System.exit(1); // shut down cluster, etc
    }

}
