package com.run.bigdata.hadoop.mr.access;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * access 流量统计driver
 */
public class AccessLocalApp {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {

        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration);

        job.setJarByClass(AccessLocalApp.class);
        job.setMapperClass(AccessMapper.class);
        job.setReducerClass(AccessReducer.class);

        // 如果在reduce结果输出中想让key不显示，定义为NullWritable，则不能在map层面使用combiner，两者的key会报冲突
//        job.setCombinerClass(AccessReducer.class);

        // 设置 分区规则
        job.setPartitionerClass(AccessPartitioner.class);
        // 设置reduce分区数
        job.setNumReduceTasks(3);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Access.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Access.class);

        //如果输出路径已经存在，要先删除
        /*FileSystem fs = FileSystem.get(new URI("hdfs://160.124.156.102:8020"),configuration,"root");
        Path outPath = new Path("output/access");
        if (fs.exists(outPath)){
            fs.delete(outPath,true);
        }*/

        //设置job输入输出文件路径
        FileInputFormat.setInputPaths(job,new Path("input/access/access.log"));
        FileOutputFormat.setOutputPath(job,new Path("output/access"));

        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : -1);
    }

}
