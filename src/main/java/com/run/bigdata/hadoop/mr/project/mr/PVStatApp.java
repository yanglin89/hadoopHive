package com.run.bigdata.hadoop.mr.project.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 流量统计 一 (每一行为一次流量)
 */
public class PVStatApp {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration configuration = new Configuration();
        Job job = new Job(configuration);

        FileSystem fileSystem = FileSystem.get(configuration);
        Path outPath = new Path(args[1]);
//        Path outPath = new Path("output/v1/pvstat");
        if (fileSystem.exists(outPath)){
            fileSystem.delete(outPath,true);
        }

        job.setJarByClass(PVStatApp.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
//        FileInputFormat.setInputPaths(job,new Path("input/raw/trackinfo_20130721.data"));
//        FileOutputFormat.setOutputPath(job,new Path("output/v1/pvstat"));

        job.waitForCompletion(true);

    }

    static class MyMapper extends Mapper<LongWritable,Text,Text,LongWritable>{

        private Text KEY = new Text("key");
        private LongWritable ONE = new LongWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            context.write(KEY,ONE);
        }
    }

    static class MyReduce extends Reducer<Text,LongWritable,NullWritable,LongWritable>{

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

            long count = 0;
            for (LongWritable value : values){
                count++; //因为每一行代表一个记录
            }

            context.write(NullWritable.get(),new LongWritable(count));

        }
    }


}












