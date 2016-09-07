package com.analytics.mapreduce;

import com.analytics.parsers.AveragesParser;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author William Smith
 */
public class AverageResponses {

    private static Configuration conf;

    public Configuration getConfiguration() {
        return conf;
    }

    public void setConfiguration(Configuration configuration) {
        this.conf = configuration;
    }

    public static class AverageMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        private StringBuilder sb = new StringBuilder(2000);
        private final AveragesParser parser = new AveragesParser(sb);
        private final Text rowKey = new Text();
        private final LongWritable latency = new LongWritable();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            sb = parser.Parse(value.toString());
            rowKey.set(sb.toString());
            latency.set(parser.getLatency());
            context.write(rowKey, latency);
            sb.setLength(0);
        }
    }

    public static class AverageReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

        private final LongWritable result = new LongWritable();
        Text rowKey = new Text();
        private final StringBuilder row = new StringBuilder(100);

        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            Long sum = 0l;
            Long count = 0l;
            for (LongWritable val : values) {
                count++;
                sum += val.get();
            }
            Long average = sum / count;
            result.set(average);
            String[] compKey = key.toString().split(":");
            if (compKey.length == 5) {
                row.append(compKey[0])
                        .append("\t")
                        .append(compKey[1])
                        .append("-")
                        .append(compKey[2])
                        .append("-")
                        .append(compKey[3])
                        .append(":")
                        .append(compKey[4]);

                rowKey.set(row.toString());
                context.write(rowKey, result);
                row.setLength(0);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (null == conf) {
            conf = new Configuration();
        }
        Job job = Job.getInstance(conf, "Averages");
        job.setJarByClass(AverageResponses.class);
        job.setMapperClass(AverageMapper.class);
        job.setReducerClass(AverageReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
        //System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
