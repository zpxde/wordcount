package com.zpx.partitioner2;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 1.定义一个类实现writable接口
 * 2.重写序列化和反序列化方法
 * 3.重写空参构造
 * 4.tostring方法
 */
public class FlowBean implements Writable{
    private long upFlow;
    private long downFlow;
    private long sumFlow;
    //空参构造
    public FlowBean() {
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long douwnFlow) {
        this.downFlow = douwnFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }
    public void setSumFlow() {
        this.sumFlow = this.upFlow+this.downFlow;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);
    }


    @Override
    public void readFields(DataInput in) throws IOException {
        this.upFlow = in.readLong();
        this.downFlow= in.readLong();
        this.sumFlow = in.readLong();

    }

    @Override
    public String toString() {
        return upFlow+"\t"+downFlow +"\t"+ sumFlow ;
    }
}
