package com.run.bigdata.hadoop.hdfs;

import java.io.IOException;
import java.util.Properties;

/**
 * 读取属性工具类
 */
public class ParamsUtil {

    private static Properties properties = new Properties();

    static {
        try {
            //配置文件在resources中，可以直接使用load方法，通过流的方式获取到
            properties.load(ParamsUtil.class.getClassLoader().getResourceAsStream("wc.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Properties getProperties(){
        return properties;
    }

    public static void main(String[] args) {
        String uri = ParamsUtil.getProperties().getProperty(Constants.HDFS_RUI);
        System.out.println(uri);
    }

}
