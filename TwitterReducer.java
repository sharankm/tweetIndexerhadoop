package com.ir.twitter;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import java.io.IOException;

public class TwitterReducer
        extends Reducer<CompositeKey, Text, Text, Text>{

    private MultipleOutputs<Text, Text> mos;

    protected void setup(Context context) throws IOException, InterruptedException {
        mos = new MultipleOutputs<Text, Text>(context);
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }

    public void reduce(CompositeKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder sb = new StringBuilder();
        for (Text val : values) {
            sb.append(val.toString()).append(",");
        }
        Text valueToEmit = new Text();
        valueToEmit.set(sb.substring(0, sb.length() - 1));
        char dirName = Character.toLowerCase(key.getKeyword().charAt(0));
        if (Character.isLetterOrDigit(dirName)) {
            mos.write(Character.toString(dirName), key.getKeyword(), valueToEmit);
        } else {
            mos.write("misc", key.getKeyword(), valueToEmit);
        }
    }
}
