package com.run.bigdata.hadoop.mr.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * KEYIN: map任务读数据的key类型，是每行数据的偏移量 offset（每一行累加） ，long
 * KEYOUT:map任务读数据的value类型，对应得是一行行的字符串  ， string
 *
 * KEYOUT:map方法自定义输出的key类型
 * VALUEOUT:map方法自定义输出的value类型
 *
 * hadoop自定义数据类型，可以实现序列化与反序列化
 * LongWritable Text IntWritable  ，分别对应java里面的long  string  int
 */
public class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //把value对应的行数据以特定的分隔符分隔
        String[] words = value.toString().toLowerCase().split(" ");

        for (String word : words){
            context.write(new Text(word),new IntWritable(1));
        }

    }
}
