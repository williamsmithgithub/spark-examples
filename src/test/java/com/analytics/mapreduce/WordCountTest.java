package com.analytics.mapreduce;

import java.io.File;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import static org.junit.Assert.*;

/**
 *
 * @author William Smith
 */
public class WordCountTest {

    YarnConfiguration clusterConf;
    MiniYARNCluster miniCluster;
    String name = "mc";
    int noOfNodeManagers = 1;
    int numLocalDirs = 1;
    int numLogDirs = 1;

    public WordCountTest() {
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    // @Before
    public void setUp() {
        clusterConf = new YarnConfiguration();
        clusterConf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 64);
        clusterConf.setClass(YarnConfiguration.RM_SCHEDULER,
                FifoScheduler.class, ResourceScheduler.class);
        miniCluster = new MiniYARNCluster(name, noOfNodeManagers, numLocalDirs, numLogDirs);
        miniCluster.init(clusterConf);
        miniCluster.start();
    }

    //@After
    public void tearDown() {
        miniCluster.stop();
    }

    /**
     * Test of main method, of class WordCount.
     */
    //@Test
    public void testMain() throws Exception {
        System.out.println("main");
        WordCount driver = new WordCount();
        driver.setConfiguration(this.miniCluster.getConfig());
        String[] args = new String[2];
        args[0] = "src/main/resources/word-count-input";
        args[1] = "word-count-output";

        File outPutDirectory = new File("word-count-output");
        String outPath = outPutDirectory.getAbsolutePath();
        if (outPutDirectory.isDirectory()) {
            FileUtils.deleteDirectory(new File(outPath));
        }

        WordCount.main(args);
        assertTrue(true);
    }

}
