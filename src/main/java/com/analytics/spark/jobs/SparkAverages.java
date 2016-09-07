package com.analytics.spark.jobs;

import com.analytics.parsers.AveragesParser;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.log4j.Logger;

import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

/**
 *
 * @author William Smith
 */
public final class SparkAverages {

    private static final Logger log = Logger.getLogger(SparkAverages.class);

    public static void main(String[] args) throws Exception {

        if (args.length < 3) {
            System.err.println("Usage: SparkAverages master <inFile> <outFile>");
            System.exit(1);
        }

        String master = args[0];
        String inPath = args[1];
        String outPath = args[2];
        String appName = "SparkAverages";

        SparkConf sparkConf;
        JavaSparkContext sc;

        if (master.equalsIgnoreCase("yarn")) {
            sparkConf = new SparkConf().setAppName(appName);
        } else {
            sparkConf = new SparkConf().setAppName(appName).setMaster(master);
        }

        sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = sc.textFile(inPath, 4);

        JavaPairRDD<String, Long> groupPairs = lines.flatMapToPair(new PairFlatMapFunction<String, String, Long>() {

            private StringBuilder sb = new StringBuilder(2000);
            private final AveragesParser parser = new AveragesParser(sb);
            

            @Override
            public Iterable<Tuple2<String, Long>> call(String line) throws Exception {
                List<Tuple2<String, Long>> retArray = new ArrayList();
                if (!line.equals("") && line != null) {
                    sb = parser.Parse(line);
                    if (!sb.toString().equals("")) {
                        Long latency = parser.getLatency();
                        Tuple2<String, Long> tuple = new Tuple2(sb.toString(), latency);
                        retArray.add(tuple);
                    }
                }
                sb.setLength(0);
                return retArray;
            }
        });

        final Map<String, Object> groupCounts = groupPairs.countByKey();
        JavaPairRDD<String, Long> groups = groupPairs.reduceByKey(new Function2<Long, Long, Long>() {

            @Override
            public Long call(Long t1, Long t2) throws Exception {
                return t1 + t2;
            }
        }, 1);

        JavaPairRDD<String, Long> rows = groups.mapToPair(new PairFunction<Tuple2<String, Long>, String, Long>() {

            private final StringBuilder row = new StringBuilder(100);

            @Override
            public Tuple2<String, Long> call(Tuple2<String, Long> tuple) throws Exception {
                Long c = (Long) groupCounts.get(tuple._1());
                Long sum = (Long) tuple._2();
                Long average = (sum / c);
                String[] compKey = tuple._1().split(":");
                row.append(compKey[0])
                        .append("\t")
                        .append(compKey[1])
                        .append("-")
                        .append(compKey[2])
                        .append("-")
                        .append(compKey[3])
                        .append(":")
                        .append(compKey[4]);

                log.info("Spark Average MR " + row.toString() + ": " + average);

                Tuple2<String, Long> tupleReturn = new Tuple2(row.toString(), average);
                row.setLength(0);
                return tupleReturn;
            }
        });

        JavaRDD<String> averagesRdd = rows.map(new Function<Tuple2<String, Long>, String>() {

            @Override
            public String call(Tuple2<String, Long> t1) throws Exception {
                return t1._1() + "\t" + t1._2();
            }
        });
        averagesRdd.saveAsTextFile(outPath);
        sc.stop();
    }
}
