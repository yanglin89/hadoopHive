package com.run.bigdata.hadoop.mr.access;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * access日志流量统计mapper
 */
public class AccessMapper extends Mapper<LongWritable,Text,Text,Access>{

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 将每一行的value拆分
        String[] lines = value.toString().split(" ");

        String ip = lines[0];
        long up = Long.parseLong(lines[lines.length - 2]);
        long down = Long.parseLong(lines[lines.length - 1]);

        context.write(new Text(ip),new Access(ip,up,down));

    }
}
