package com.run.bigdata.hadoop.hdfs;

import java.util.HashMap;
import java.util.Map;

/**
 * 自定义缓存
 */
public class ContentCache {

    private Map<Object,Object> cacheMap = new HashMap<>();

    /**
     * 对外暴露接口让用户可以获取到缓存map
     * @return
     */
    public Map<Object,Object> getCacheMap(){
        return cacheMap;
    }


    public void write(Object key,Object value){
        cacheMap.put(key,value);
    }

    public Object get(Object key){
        return cacheMap.get(key);
    }

}
