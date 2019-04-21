package com.run.bigdata.hadoop.hdfs;

/**
 * 词频统计mapper
 */
public interface WCMapper {

    public void map(String line,ContentCache content);

}
