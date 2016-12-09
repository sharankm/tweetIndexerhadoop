package com.ir.twitter;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.mapreduce.Partitioner;

public class KeyPartitioner extends Partitioner<CompositeKey, Text> {
    HashPartitioner<Text, Text> hashPartitioner = new HashPartitioner<Text, Text>();
    Text newKey = new Text();

    @Override
    public int getPartition(CompositeKey key, Text value, int numReduceTasks) {

        try {
            newKey.set(key.getKeyword());
            return hashPartitioner.getPartition(newKey, value, numReduceTasks);
        } catch (Exception e) {
            e.printStackTrace();
            return (int) (Math.random() * numReduceTasks);
        }
    }
}
