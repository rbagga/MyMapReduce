import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Covid {

    public static class MapOne extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] info = line.split(" ");
            if (info.length == 4) {
                context.write(new Text(info[0]), new Text(info[1] + "," + info[2] + "," + info[3]));
            }
        }
    }

    public static class MapTwo extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(value, new Text("positive"));
        }
    }

    public static class Combiner extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
            String bigVal = "";
            for (Text val : values) {
                bigVal += val.toString() + ",";
            }
            context.write(key, new Text(bigVal.substring(0, bigVal.length() - 1)));
        }
    }

    public static class MapThree extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] info = value.toString().split("\t");
            String line = info[1].toString();
            String name = info[0];
            String[] val = line.split(",");
            if (val.length > 2) {
                if (line.contains("positive")) {
                    context.write(new Text(val[0]), new Text(val[3] + "," + val[1] + "," + val[2]));
                } else {
                    context.write(new Text(val[0]), new Text("testcase," + val[1] + "," + val[2] + "," + name));
                }
            }
        }
    }

    public static class ReduceThree extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
            ArrayList<String> positives = new ArrayList<String>();
            ArrayList<String> toTest = new ArrayList<String>();
            for (Text val : values) {
                String person = val.toString();
                if (person.contains("positive")) {
                    positives.add(person);
                } else {
                    toTest.add(person);
                }
            }
            
            for (String pos : positives) {
                String[] info = pos.split(",");
                int posStart = Integer.parseInt(info[1]);
                int posEnd = Integer.parseInt(info[2]);
                for (String test : toTest) {
                    String[] pinfo = test.split(",");
                    int pStart = Integer.parseInt(pinfo[1]);
                    int pEnd = Integer.parseInt(pinfo[2]);
                    if (pStart < posEnd || posStart < pEnd) {
                        context.write(new Text(pinfo[3]), new Text("null"));
                    }
                }
            }
        }
    }

    public static class MapFour extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] info = value.toString().split("\t");
            context.write(new Text(info[0]), new Text(info[1]));
        }
    }

    public static class ReduceFour extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
            context.write(key, new Text("Needs to be tested"));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "covid12");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setReducerClass(Combiner.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        MultipleInputs.addInputPath(job, new Path("D1"), TextInputFormat.class, MapOne.class);
        MultipleInputs.addInputPath(job, new Path("D2"), TextInputFormat.class, MapTwo.class);
        FileOutputFormat.setOutputPath(job, new Path("out3"));

        job.setJarByClass(Covid.class);
        job.waitForCompletion(true);

        Job job2 = new Job(conf, "covid3");

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        
        job2.setMapperClass(MapThree.class);
        job2.setReducerClass(ReduceThree.class);

        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job2, new Path("out3"));
        FileOutputFormat.setOutputPath(job2, new Path("out4"));

        job2.setJarByClass(Covid.class);
        job2.waitForCompletion(true);

        Job job3 = new Job(conf, "covid4");

        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        
        job3.setMapperClass(MapFour.class);
        job3.setReducerClass(ReduceFour.class);

        job3.setInputFormatClass(TextInputFormat.class);
        job3.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job3, new Path("out4"));
        FileOutputFormat.setOutputPath(job3, new Path("covid_output"));

        job3.setJarByClass(Covid.class);
        job3.waitForCompletion(true);
    }
}
