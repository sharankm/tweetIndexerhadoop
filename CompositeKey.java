package com.ir.twitter;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CompositeKey implements WritableComparable {

    private String keyword;
    private String followers;

    public CompositeKey() {
    }

    public CompositeKey(String keyword, String followers) {
        this.keyword = keyword;
        this.followers = followers;
    }

    @Override
    public String toString() {

        return (new StringBuilder()).append(keyword).append(',').append(followers).toString();
    }

    public void readFields(DataInput in) throws IOException {

        keyword = WritableUtils.readString(in);
        followers = WritableUtils.readString(in);
    }

    public void write(DataOutput out) throws IOException {

        WritableUtils.writeString(out, keyword);
        WritableUtils.writeString(out, followers);
    }

    public int compareTo(Object o) {
        CompositeKey comp = (CompositeKey)o;
        int result = keyword.compareTo(comp.keyword);
        if (0 == result) {
            result = followers.compareTo(comp.followers);
        }
        return result;
    }

    public String getKeyword() {
        return keyword;
    }

    public void setKeyword(String keyword) {
        this.keyword = keyword;
    }

    public String getFollowers() {
        return followers;
    }

    public void setFollowers(String followers) {
        this.followers = followers;
    }
}