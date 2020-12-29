import java.io.IOException;
import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Condorcet {

    public static final Log log = LogFactory.getLog(Condorcet.class);

    public static class MapOne extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private final static IntWritable zero = new IntWritable(0);

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String vote = value.toString();
            int m = 3;
            vote = vote.replaceAll(" ", "");
            for (int i = 0; i < m-1; i++) {
                for (int j = i+1; j < m; j++) {
                    if (vote.charAt(i) < vote.charAt(j)) {
                        Text word = new Text(vote.charAt(i) + "," + vote.charAt(j));
                        context.write(word, one);     
                    } else {
                        Text word = new Text(vote.charAt(i) + "," + vote.charAt(j));
                        context.write(word, zero);
                    }
                }
            }
        }
    }

    public static class ReduceOne extends Reducer<Text, IntWritable, Text, Text> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
            String pair = key.toString();
            int ones = 0;
            int zeros = 0;
            String[] cands = pair.split(",", 0);
            for (IntWritable val : values) {
                if (val.get() == 1) {
                    ones += 1;
                } else {
                    zeros += 1;
                }
            }
            Text first = new Text(cands[0]);
            Text second = new Text(cands[1]);
            if (ones > zeros) {
                context.write(first, second);
            } else {
                context.write(second, first);
            }
        }
    }

    public static class MapTwo extends Mapper<LongWritable, Text, IntWritable, Text> {
        private final static IntWritable one = new IntWritable(1);
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] cands = value.toString().split("\t");
            Text val = new Text(cands[0] + "," + cands[1]);
            context.write(one, val);
        }
    }

    public static class ReduceTwo extends Reducer<IntWritable, Text, Text, Text> {

        public void reduce(IntWritable key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
            int m = 3;
            int maxcount = Integer.MIN_VALUE;
            HashMap<String, Integer> candidates = new HashMap<String, Integer>();
            
            for (Text val : values) {
                String first = val.toString().split(",")[0];
                candidates.put(first, candidates.getOrDefault(first, 0) + 1);
                if (candidates.get(first) == m-1) {
                    context.write(new Text(first), new Text("Condorcet winner!"));
                    return;
                }
                if (candidates.get(first) > maxcount) {
                    maxcount = candidates.get(first);
                }
            }

            for (String cand : candidates.keySet()) {
                if (candidates.get(cand) == maxcount) {
                    context.write(new Text(cand), new Text("No Condorcet winner, Highest Condorcet count"));
                }
            }
            
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "condorcet1");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(MapOne.class);
        job.setReducerClass(ReduceOne.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path("out"));

        job.setJarByClass(Condorcet.class);
        job.waitForCompletion(true);

        Job job2 = new Job(conf, "condorcet2");

        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(Text.class);
        
        job2.setMapperClass(MapTwo.class);
        job2.setReducerClass(ReduceTwo.class);

        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job2, new Path("out"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));

        job2.setJarByClass(Condorcet.class);
        job2.waitForCompletion(true);
    }
}