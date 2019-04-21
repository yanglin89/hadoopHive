package com.run.bigdata.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * 使用hdfs api 完成word count 统计
 *
 * 读取dhfs文件
 * 业务处理
 * 将处理结果缓存
 * 输出结果文件到hdfs
 */
public class HDFSWCApp01 {

    public static void main(String[] args) throws Exception{
        Path input = new Path("/hdfsapi/test/wordcount.txt");

        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop000:8020"),new Configuration(),"root");

        RemoteIterator<LocatedFileStatus> files =  fs.listFiles(input,true);

        ContentCache content = new ContentCache(); //自定义上下文，用作缓存
        WordCountMapper mapper = new WordCountMapper();

        while (files.hasNext()){
            LocatedFileStatus file = files.next();
            FSDataInputStream in = fs.open(file.getPath());
            BufferedReader br = new BufferedReader(new InputStreamReader(in));

            String line = "";
            while ((line = br.readLine()) != null){
                //词频处理,同时将结果缓存起来
                mapper.map(line,content);
            }

            br.close();
            in.close();
        }


        // 从自定义缓存中获取到结果
        Map<Object,Object> contentMap = content.getCacheMap();

        Path output = new Path("/hdfsapi/output");

        FSDataOutputStream out = fs.create(new Path(output,new Path("wc.out")));
        //将缓存中的内容输出到out
        Set<Map.Entry<Object,Object>> entries = contentMap.entrySet();
        for (Map.Entry<Object,Object> entry : entries){
            out.write((entry.getKey().toString()+ "\t" + entry.getValue() + "\n").getBytes());
        }

        System.out.println("word count 统计完成");
    }
}
