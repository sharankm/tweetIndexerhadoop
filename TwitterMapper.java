package com.ir.twitter;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.StringTokenizer;

public class TwitterMapper extends Mapper<Object, Text, CompositeKey, Text> {

    String filename;
    int offset = 0;

    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit fs = (FileSplit) context.getInputSplit();
        filename = fs.getPath().getName();
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        int offsetAdd = 0;
        String input = value.toString();
        String[] lines = input.split("\n");
        String followersCount = "";
        for (int i = 0; i < lines.length; i++) {
            String line = lines[i];
            offsetAdd += line.length() + 1;
            String field = "";
            String info = "";
            if (line.contains("|")) {
                String[] lineSplit = line.split("\\|");
                field = lineSplit[0].trim();
                if (lineSplit.length > 1)
                    info = lineSplit[1].trim();
            }
            if (field.equals(""))
                info = line;
            if (field.equals("#Followers")) {
                followersCount = info;
            }
            if (field.equals("Status") || field.equals("") || (field.equals("Hashtags") && !info.equals(""))) {
                StringTokenizer itr = new StringTokenizer(info);
                while (itr.hasMoreTokens()) {
                    String nextToken = itr.nextToken().toLowerCase();
                    String nextWord = nextToken.replaceAll("[^\\w\\s\\#\\@]", "");
                    if (!(nextWord.contains("http") || StopWordList.getStopWordList().contains(nextWord) || (nextWord.trim().length() == 0))) {
                        String statusPos = String.valueOf(offset);
                        String outString = filename + "|" + statusPos + "|" + followersCount;
                        CompositeKey compKey = new CompositeKey();
                        compKey.setFollowers(followersCount);
                        compKey.setKeyword(nextWord);
                        context.write(compKey, new Text(outString));
                    }
                }
            }
        }
        offset = offset + offsetAdd + 1;
    }
}
