package com.zpx.outputformat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class LogReducer extends Reducer<Text,NullWritable,Text,NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

        //http://www.baidu.com
        //http://www.google.com
        //防止有相同数据丢数据
        for (NullWritable value : values) {
            context.write(key,NullWritable.get());
        }

    }
}
