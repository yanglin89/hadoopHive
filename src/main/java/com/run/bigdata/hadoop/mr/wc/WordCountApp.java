package com.run.bigdata.hadoop.mr.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * 词频统计driver ，配置mr的相关属性
 */
public class WordCountApp {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {

        //设置hadoop用户
        System.setProperty("HADOOP_USER_NAME","root");

        //hdfs相关设置
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://160.124.156.102:8020");

        //创建一个job
        Job job = Job.getInstance(configuration);

        //设置job先关参数:主类
        job.setJarByClass(WordCountApp.class);

        //设置job先关参数:mapper 、 reducer
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        //设置map输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //设置reduce输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //如果输出路径已经存在，要先删除
        FileSystem fs = FileSystem.get(new URI("hdfs://160.124.156.102:8020"),configuration,"root");
        Path outPath = new Path("/wordcount/output");
        if (fs.exists(outPath)){
            fs.delete(outPath,true);
        }

        //设置job输入输出文件路径
        FileInputFormat.setInputPaths(job,new Path("/wordcount/input"));
        FileOutputFormat.setOutputPath(job,outPath);

        //提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : -1);

    }

}





















