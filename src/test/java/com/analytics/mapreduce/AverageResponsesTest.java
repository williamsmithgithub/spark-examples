package com.analytics.mapreduce;

import java.io.File;
import org.apache.commons.io.FileUtils;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author William Smith
 */
public class AverageResponsesTest {

    YarnConfiguration clusterConf;
    MiniYARNCluster miniCluster;
    String name = "mc";
    int noOfNodeManagers = 2;
    int numLocalDirs = 1;
    int numLogDirs = 1;

    public AverageResponsesTest() {
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {
        clusterConf = new YarnConfiguration();
        clusterConf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 64);
        clusterConf.setClass(YarnConfiguration.RM_SCHEDULER,
                FifoScheduler.class, ResourceScheduler.class);
        miniCluster = new MiniYARNCluster(name, noOfNodeManagers, numLocalDirs, numLogDirs);
        miniCluster.init(clusterConf);
        miniCluster.start();
    }

    @After
    public void tearDown() {
        miniCluster.stop();
    }

    /**
     * Test of main method, of class WordCount.
     */
    @Test
    public void testMain() throws Exception {
        System.out.println("main");
        AverageResponses driver = new AverageResponses();
        driver.setConfiguration(this.miniCluster.getConfig());
        String[] args = new String[2];
        args[0] = "src/main/resources/input";
        args[1] = "hadoop-average-output";

        File outPutDirectory = new File("hadoop-average-output");
        String outPath = outPutDirectory.getAbsolutePath();
        if (outPutDirectory.isDirectory()) {
            FileUtils.deleteDirectory(new File(outPath));
        }

        AverageResponses.main(args);
        assertTrue(true);
    }

}
