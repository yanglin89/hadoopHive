package com.run.bigdata.hadoop.hdfs;

/**
 * 自定义的词频统计实现类
 */
public class WordCountMapper implements WCMapper{

    @Override
    public void map(String line, ContentCache content) {
        String[] words = line.split(" ");

        for (String word : words){
            Object value = content.get(word);
            if (value == null){
                content.write(word, 1);
            } else {
                int v = Integer.parseInt(value.toString());
                content.write(word, v + 1);
            }
        }
    }
}
