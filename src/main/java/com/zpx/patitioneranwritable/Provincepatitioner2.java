package com.zpx.patitioneranwritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class Provincepatitioner2 extends Partitioner<FlowBean,Text> {
    @Override
    public int getPartition(FlowBean flowBean, Text text, int i) {
        String phone = text.toString();
        String prePhone = phone.substring(0,3);
        int partition;
        if ("136".equals(prePhone))
        {
            return 0;
        } else if ("137".equals(prePhone)) {
            return 1;
        } else if ("138".equals(prePhone)) {
            return 2;
        } else if ("139".equals(prePhone)) {
            return 3;
        } else {
            return 4;
        }


    }
}
