package org.apache.ratis.inst;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.ratis.inst.FailureInjector.FailureType.*;

public class TestConfig {
    public static final Logger LOG = LoggerFactory.getLogger(TestConfig.class);

    private static TestConfig INSTANCE;

    // cluster config
    public final String CONN_ADAPTER;
    public final int NUM_SERVERS;
    public final int SNAPSHOT_TRIGGER_THRESHOLD;
    public final int PURGE_GAP;
    public final int NO_LEADER_TIMEOUT;
    public final int CLIENT_REQ_TIMEOUT;

    // test config
    public final int TEST_ID;

    public final int PHASE_LENGTH;

    // failures config
    public final String FAILURE_INJECTOR_TYPE;
    public final int RANDOM_SEED;
    public final int NUM_FAILURES;
    public final int NUM_REQUESTS;
    public final int MAX_NUM_ROUNDS;
    public final int FAILURE_RECOVERY_PERIOD;
    public final double DROP_WITH_PROB;

    // output config
    public final String OUT_FOLDER_BASE_NAME;
    public final String OUT_FILE_BASE_NAME;

    public final String OUTPUT_FOLDER_NAME; // basename + conn+adapter + numFailures
    public final String OUTPUT_FILE_NAME; // basename + testId

    private TestConfig(String configFile, String[] args) {
        Properties prop = loadProperties(configFile);
        Map<String, String> overrideArgs = new HashMap<>();

        if(args != null && args.length != 0) {
            overrideArgs = Arrays.stream(args)
                    .filter(s -> s.contains("="))
                    .map(s -> Arrays.asList(s.split("=")))
                    .collect(Collectors.toMap(kv -> kv.get(0), kv -> kv.get(1)));
        }

        CONN_ADAPTER = overrideArgs.getOrDefault("connAdapter", prop.getProperty("connAdapter"));

        NUM_SERVERS =  Integer.parseInt(overrideArgs.getOrDefault("numServers", prop.getProperty("numServers")));
        SNAPSHOT_TRIGGER_THRESHOLD =  Integer.parseInt(overrideArgs.getOrDefault("snapshotTriggerThreshold", prop.getProperty("snapshotTriggerThreshold")));
        PURGE_GAP =  Integer.parseInt(overrideArgs.getOrDefault("purgeGap", prop.getProperty("purgeGap")));
        NO_LEADER_TIMEOUT = Integer.parseInt(overrideArgs.getOrDefault("noLeaderTimeout", prop.getProperty("noLeaderTimeout")));
        CLIENT_REQ_TIMEOUT = Integer.parseInt(overrideArgs.getOrDefault("clientReqTimeout", prop.getProperty("clientReqTimeout")));

        TEST_ID =  Integer.parseInt(overrideArgs.getOrDefault("testId", prop.getProperty("testId")));
        RANDOM_SEED =  Integer.parseInt(overrideArgs.getOrDefault("randomSeed", prop.getProperty("randomSeed")));

        FAILURE_INJECTOR_TYPE = overrideArgs.getOrDefault("failureInjector", prop.getProperty("failureInjector"));

        DROP_WITH_PROB = Double.parseDouble(overrideArgs.getOrDefault("dropWithProb", prop.getProperty("dropWithProb")));

        NUM_REQUESTS = Integer.parseInt(overrideArgs.getOrDefault("numRequests", prop.getProperty("numRequests")));
        NUM_FAILURES = Integer.parseInt(overrideArgs.getOrDefault("numFailures", prop.getProperty("numFailures")));
        FAILURE_RECOVERY_PERIOD = Integer.parseInt(overrideArgs.getOrDefault("failureRecoveryPeriod", prop.getProperty("failureRecoveryPeriod")));
        MAX_NUM_ROUNDS = Integer.parseInt(overrideArgs.getOrDefault("maxNumRounds", prop.getProperty("maxNumRounds")));
        PHASE_LENGTH = Integer.parseInt(overrideArgs.getOrDefault("phaseLength", prop.getProperty("phaseLength")));

        OUT_FOLDER_BASE_NAME = overrideArgs.getOrDefault("outFolderBaseName", prop.getProperty("outFolderBaseName"));
        OUT_FILE_BASE_NAME = overrideArgs.getOrDefault("outFileBaseName", prop.getProperty("outFileBaseName"));

        String outFolder = OUT_FOLDER_BASE_NAME;

        if(FAILURE_INJECTOR_TYPE.equals(ARBITRARY_LINK_FAILURES.toString())) {
            outFolder = "Arb" + "-" + CONN_ADAPTER + "-p" + DROP_WITH_PROB + "-" + outFolder;
        } else if(FAILURE_INJECTOR_TYPE.equals(UNIFORM_SYNC_FAILURES_WHITEBOX.toString())) {
            outFolder = "UniWhite" + "-" + CONN_ADAPTER + "-d" + NUM_FAILURES + "-" + outFolder;
        } else if(FAILURE_INJECTOR_TYPE.equals(UNIFORM_SYNC_FAILURES_BLACKBOX_API.toString())) {
            outFolder = "UniAPI" + "-" + CONN_ADAPTER + "-d" + NUM_FAILURES + "-" + outFolder;
        } else if(FAILURE_INJECTOR_TYPE.equals(UNIFORM_SYNC_FAILURES_BLACKBOX_RESTART.toString())) {
            outFolder = "UniRestart" + "-" + CONN_ADAPTER + "-d" + NUM_FAILURES + "-" + outFolder;
        }
        OUTPUT_FOLDER_NAME = outFolder;
        OUTPUT_FILE_NAME = OUT_FILE_BASE_NAME + TEST_ID;
    }

    private static Properties loadProperties(String configFile) {
        Properties prop = new Properties();
        try (FileInputStream ip = new FileInputStream(configFile)) {
            prop.load(ip);
        } catch (IOException e) {
            LOG.error("Cannot load properties file: " + configFile);
        }
        return prop;
    }

    public synchronized static TestConfig initialize(String configFile, String[] args) {
        INSTANCE = new TestConfig(configFile, args);
        return INSTANCE;
    }

    public synchronized static TestConfig getInstance() {
        if (INSTANCE == null) {
            throw new IllegalStateException("Configuration not initialized");
        }
        return INSTANCE;
    }
}
