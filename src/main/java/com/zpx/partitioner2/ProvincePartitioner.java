package com.zpx.partitioner2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvincePartitioner extends Partitioner<Text,FlowBean>{
    @Override
    public int getPartition(Text text, FlowBean flowBean, int i) {
        //text是手机号
        String phone = text.toString();
        String prePhone = phone.substring(0, 3);
        int partiton;
        if("136".equals(prePhone)) {
        partiton=0;
        }else if("137".equals(prePhone)) {
            partiton=1;
        } else if("138".equals(prePhone)) {
            partiton=2;
        }else if("139".equals(prePhone)) {
            partiton=3;
        }else{
            partiton=4;
        }
        return partiton;
    }
}
