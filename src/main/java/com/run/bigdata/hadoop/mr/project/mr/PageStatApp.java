package com.run.bigdata.hadoop.mr.project.mr;

import com.run.bigdata.hadoop.mr.project.Utils.ContentUtils;
import com.run.bigdata.hadoop.mr.project.Utils.IPParser;
import com.run.bigdata.hadoop.mr.project.Utils.LogParse;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Map;

public class PageStatApp {

    public static void main(String[] args) throws Exception{

        Configuration configuration = new Configuration();
        Job job = new Job(configuration);

        FileSystem fileSystem = FileSystem.get(configuration);
//        Path outPath = new Path("output/v1/pagestat/");
        Path outPath = new Path(args[1]);
        if (fileSystem.exists(outPath)){
            fileSystem.delete(outPath,true);
        }

        job.setJarByClass(PageStatApp.class);

        job.setMapperClass(PageStatApp.MyMapper.class);
        job.setReducerClass(PageStatApp.MyReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
//        FileInputFormat.setInputPaths(job,new Path("input/raw/trackinfo_20130721.data"));
//        FileOutputFormat.setOutputPath(job,new Path("output/v1/pagestat"));

        job.waitForCompletion(true);

    }

    static class MyMapper extends Mapper<LongWritable,Text,Text,LongWritable> {

        private LongWritable ONE = new LongWritable(1);

        private LogParse logParse;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            logParse = new LogParse();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String log = value.toString();

            Map<String,String> info = logParse.parse(log);
            String url = info.get("url");

            if (StringUtils.isNotBlank(url)){
                String pageId = ContentUtils.getPageId(url);
                context.write(new Text(pageId),ONE);
            }else {
                context.write(new Text("-"),ONE);
            }

        }
    }

    static class MyReduce extends Reducer<Text,LongWritable,Text,LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long count = 0;
            for (LongWritable value : values){
                count++;
            }

            context.write(key,new LongWritable(count));
        }
    }

}
