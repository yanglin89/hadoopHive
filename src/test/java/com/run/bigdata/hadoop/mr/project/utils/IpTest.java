package com.run.bigdata.hadoop.mr.project.utils;

import com.run.bigdata.hadoop.mr.project.Utils.IPParser;
import org.junit.Test;

public class IpTest {

    @Test
    public void testIp(){

        IPParser.RegionInfo regionInfo = IPParser.getInstance().analyseIp("192.168.52.128");
        System.out.println(regionInfo.getCountry());
        System.out.println(regionInfo.getProvince());
        System.out.println(regionInfo.getCity());

    }

}
