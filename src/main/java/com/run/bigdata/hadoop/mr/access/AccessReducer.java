package com.run.bigdata.hadoop.mr.access;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 日志分析reducer
 * 输出key使用NullWritable，用于在结果中不展示
 */
public class AccessReducer extends Reducer<Text,Access,NullWritable,Access>{

    /**
     * @param key ip
     * @param values hadoop通过map的输出key -- ip，已经聚合好的access的集合
     */
    @Override
    protected void reduce(Text key, Iterable<Access> values, Context context) throws IOException, InterruptedException {

        long ups = 0L;
        long downs = 0L;

        for(Access access : values){
            ups += access.getUp();
            downs += access.getDown();
        }

        context.write(NullWritable.get(),new Access(key.toString(),ups,downs));
    }
}
