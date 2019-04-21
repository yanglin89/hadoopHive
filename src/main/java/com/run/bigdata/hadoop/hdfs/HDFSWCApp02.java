package com.run.bigdata.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * 使用hdfs api 完成word count 统计
 *
 * 读取dhfs文件
 * 业务处理
 * 将处理结果缓存
 * 输出结果文件到hdfs
 */
public class HDFSWCApp02 {

    public static void main(String[] args) throws Exception{

        Properties properties = ParamsUtil.getProperties();
        Path input = new Path(properties.getProperty(Constants.INPUT_PATH));

        FileSystem fs = FileSystem.get(new URI(properties.getProperty(Constants.HDFS_RUI)),new Configuration(),properties.getProperty(Constants.HDFS_USER));

        RemoteIterator<LocatedFileStatus> files =  fs.listFiles(input,true);

        ContentCache content = new ContentCache(); //自定义上下文，用作缓存
//        WordCountMapper mapper = new WordCountMapper();
        // 通过反射的方式获取到mapper，实现类写在配置文件中，方便配置和切换,
        // 实现业务逻辑的可插拔，我们只需要根据不同的业务逻辑场景在配置文件中配置不同的实现类即可（忽略大小写），不用硬编码new一个实体在代码中
        Class<?> clazz = Class.forName(properties.getProperty(Constants.WC_MAPPER));
        WCMapper mapper = (WCMapper) clazz.newInstance();

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

        Path output = new Path(properties.getProperty(Constants.OUTPUT_PATH));

        FSDataOutputStream out = fs.create(new Path(output,new Path(properties.getProperty(Constants.OUTPUT_NAME))));
        //将缓存中的内容输出到out
        Set<Map.Entry<Object,Object>> entries = contentMap.entrySet();
        for (Map.Entry<Object,Object> entry : entries){
            out.write((entry.getKey().toString()+ "\t" + entry.getValue() + "\n").getBytes());
        }

        System.out.println("word count 统计完成");
    }
}
