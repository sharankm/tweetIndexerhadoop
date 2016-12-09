package com.ir.twitter;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class KeyComparator extends WritableComparator {
    protected KeyComparator() {
        super(CompositeKey.class, true);
    }
    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {

        CompositeKey key1 = (CompositeKey) w1;
        CompositeKey key2 = (CompositeKey) w2;
        int compare = key1.getKeyword().compareTo(key2.getKeyword());

        if (compare == 0) {
            if(!key1.getFollowers().isEmpty() && !key2.getFollowers().isEmpty()){
                Integer int1 = Integer.parseInt(key1.getFollowers());
                Integer int2 = Integer.parseInt(key2.getFollowers());
                return int2.compareTo(int1);
            } else{
                return key2.getFollowers().compareTo(key1.getFollowers());
            }
        }
        return compare;
    }

}
