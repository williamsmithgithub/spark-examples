package com.analytics.spark.jobs;

import java.io.File;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author hadoop
 */
public class SparkAveragesTest {
    
    public SparkAveragesTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }

    /**
     * Test of main method, of class SparkAverages.
     */
    @Test
    public void testMain() throws Exception {
        System.out.println("main");
        File inputDirectory = new File("src/main/resources/input");
        String inPath = inputDirectory.getAbsolutePath();
        
        File file = new File("spark-average-output");
        String outPath = file.getAbsolutePath();
        if (file.isDirectory()) {
            FileUtils.deleteDirectory(new File(outPath));
        }

        String[] args = {"local[4]", inPath, outPath};
        SparkAverages.main(args);
        assertTrue(true);
    }
    
}
