package com.run.bigdata.hadoop.mr.access;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * mapreduce 自定义分区规则
 */
public class AccessPartitioner extends Partitioner<Text, Access> {

    /**
     * @param text ip
     * @param i 分为几个分区
     * @return
     */
    @Override
    public int getPartition(Text text, Access access, int i) {

        // java 中点是一个关键字符，需要转义
        int flag = Integer.parseInt(text.toString().split("\\.")[0]);

        if (flag < 100) {
            return 0;
        }else if (flag < 150){
            return 1;
        }else {
            return 2;
        }
    }
}
