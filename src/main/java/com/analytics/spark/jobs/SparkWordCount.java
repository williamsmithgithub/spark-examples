package com.analytics.spark.jobs;


import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.log4j.Logger;

/**
 *
 * @author William Smith
 */
public final class SparkWordCount {

    private static final Logger log = Logger.getLogger(SparkWordCount.class);
    private static final Pattern SPACE = Pattern.compile(" ");

    /**
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        if (args.length < 3) {
            System.err.println("Usage: SparkWordCount master <inFile> <outFile>");
            System.exit(1);
        }

        String master = args[0];
        String inPath = args[1];
        String outPath = args[2];
        String appName = "SparkWordCount";

        SparkConf sparkConf;
        JavaSparkContext sc;
        
        if (master.equalsIgnoreCase("yarn")) {
            sparkConf = new SparkConf().setAppName(appName);
        } else {
            sparkConf = new SparkConf().setAppName(appName).setMaster(master);
        }
        
        sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = sc.textFile(inPath, 4);

        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) {
                return Arrays.asList(SPACE.split(s));
            }
        });

        JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        });

        List<Tuple2<String, Integer>> output = counts.collect();
        for (Tuple2<?, ?> tuple : output) {
            log.info("Spark WordCount MR " + tuple._1() + ": " + tuple._2());
            System.out.println("Spark WordCount MR " + tuple._1() + ": " + tuple._2());
        }
        ones.saveAsTextFile(outPath);
        sc.stop();
    }
}
