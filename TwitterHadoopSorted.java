package com.ir.twitter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TwitterHadoopSorted extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new TwitterHadoopSorted(), args);
        System.exit(exitCode);
    }

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("textinputformat.record.delimiter", "\n\r");
        Job job = Job.getInstance(conf);
        job.setJarByClass(TwitterHadoopSorted.class);
        job.setMapperClass(TwitterMapper.class);
        job.setReducerClass(TwitterReducer.class);
        job.setMapOutputKeyClass(CompositeKey.class);
        job.setPartitionerClass(KeyPartitioner.class);
        job.setGroupingComparatorClass(KeyGroupingComparator.class);
        job.setSortComparatorClass(KeyComparator.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        for (int i = 0; i <= 9; i++)
            MultipleOutputs.addNamedOutput(job, Integer.toString(i), TextOutputFormat.class, Text.class, Text.class);
        for (char alphabet1 = 'a'; alphabet1 <= 'z'; alphabet1++) {
            MultipleOutputs.addNamedOutput(job, Character.toString(alphabet1), TextOutputFormat.class, Text.class, Text.class);
        }
        MultipleOutputs.addNamedOutput(job, "misc", TextOutputFormat.class, Text.class, Text.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }
}