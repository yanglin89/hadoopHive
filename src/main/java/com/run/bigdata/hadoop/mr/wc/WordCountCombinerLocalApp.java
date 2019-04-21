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
 * 添加combiner  求和的时候，combiner是没有问题的，但是如果是求平均数之类的做除法运算的时候，combiner回导致结果的不准确
 * 在本地文件系统测试
 */
public class WordCountCombinerLocalApp {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {

        //hdfs相关设置
        Configuration configuration = new Configuration();

        //创建一个job
        Job job = Job.getInstance(configuration);

        //设置job先关参数:主类
        job.setJarByClass(WordCountCombinerLocalApp.class);

        //设置job先关参数:mapper 、 reducer
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        //添加combiner
        job.setCombinerClass(WordCountReducer.class);

        //设置map输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //设置reduce输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //如果输出路径已经存在，要先删除
        /*FileSystem fs = FileSystem.get(new URI("hdfs://160.124.156.102:8020"),configuration,"root");
        Path outPath = new Path("/wordcount/output");
        if (fs.exists(outPath)){
            fs.delete(outPath,true);
        }*/

        //设置job输入输出文件路径
        FileInputFormat.setInputPaths(job,new Path("input/wc"));
        FileOutputFormat.setOutputPath(job,new Path("output/wc"));

        //提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : -1);

    }

}





















