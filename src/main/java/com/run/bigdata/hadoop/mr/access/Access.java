package com.run.bigdata.hadoop.mr.access;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自定义复杂数据类型
 * 按照hadoop规范，自定义数据类型，必须实现writable接口，并实现其方法
 * 同时还要定义一个默认的构造方法
 */
public class Access implements Writable{
    private String ip; //ip
    private long up; //上行流量
    private long down; //下行流量
    private long sum; //流量和

    @Override
    public void write(DataOutput out) throws IOException {
        //根据属性的类型来选择write 的类型
        out.writeUTF(ip);
        out.writeLong(up);
        out.writeLong(down);
        out.writeLong(sum);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        //out 先写那个，in就要先读哪个
        this.ip = in.readUTF();
        this.up = in.readLong();
        this.down = in.readLong();
        this.sum = in.readLong();
    }

    public Access() {
    }

    public Access(String ip, long up, long down) {
        this.ip = ip;
        this.up = up;
        this.down = down;
        this.sum = up + down;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public long getUp() {
        return up;
    }

    public void setUp(long up) {
        this.up = up;
    }

    public long getDown() {
        return down;
    }

    public void setDown(long down) {
        this.down = down;
    }

    public long getSum() {
        return sum;
    }

    public void setSum(long sum) {
        this.sum = sum;
    }

    @Override
    public String toString() {
        return "ip='" + ip + '\'' +
                ", up=" + up +
                ", down=" + down +
                ", sum=" + sum;
    }
}
