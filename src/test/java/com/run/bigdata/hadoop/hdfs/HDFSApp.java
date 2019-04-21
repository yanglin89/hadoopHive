package com.run.bigdata.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;

/**
 * 使用java api操作hdfs文件系统
 * 1、第一步 创建Configuration
 * 2、第二步 创建FileSystem
 * 3、第三步 java api操作
 */
public class HDFSApp {

    public static final String HDFS_URI = "hdfs://master:9000";
    Configuration configuration = null;
    FileSystem fileSystem =null;

    @Before
    public void beforeStart() throws Exception{
        System.out.println("------------before start--------------");
        configuration = new Configuration();
        configuration.set("dfs.replication","1");

        fileSystem = FileSystem.get(new URI(HDFS_URI),configuration,"hadoop");
    }

    @After
    public void afterOver(){
        configuration = null;
        fileSystem = null;
        System.out.println("------------after over--------------");
    }

    @Test
    public void testMkdir() throws Exception{
        Path path = new Path("/hdfsapi/test2");
        boolean result = fileSystem.mkdirs(path);

        System.out.println(result);
    }

    @Test
    public void testText() throws Exception{
        Path path = new Path("/hdfs-test/README.txt");
        FSDataInputStream in = fileSystem.open(path);
        IOUtils.copyBytes(in,System.out,1024);
    }

    @Test
    public void testCreate() throws Exception{
        Path path = new Path("/hdfsapi/test/bb.txt");
        FSDataOutputStream out = fileSystem.create(path);
        out.writeUTF("hello hadoop , hello bigdata");
        out.flush();
        out.close();
    }

    @Test
    public void testReplication(){
        String count = configuration.get("dfs.replication");
        System.out.println(count);
    }

    @Test
    public void testRename() throws IOException {
        Path oldPath = new Path("/hdfsapi/test/bb.txt");
        Path newPath = new Path("/hdfsapi/test/cc.txt");
        boolean result = fileSystem.rename(oldPath,newPath);
        System.out.println(result);
    }

    @Test
    public void testCopyFromLocalFile() throws IOException {
        Path src = new Path("E:/wordcount.txt");
        Path dst = new Path("/hdfsapi/test/wordcount.txt");
        fileSystem.copyFromLocalFile(src,dst);
    }

    @Test
    public void testCopyFromLocalBigFile() throws IOException {
        InputStream in = new BufferedInputStream(new FileInputStream(new File("E:\\java视频\\尚学堂线下培训教程\\12-26-linux\\linux.rar")));
        FSDataOutputStream out = fileSystem.create(new Path("/hdfsapi/test/linux.tar.gz.test"), new Progressable() {
            @Override
            public void progress() {
                System.out.print("...");
            }
        });

        IOUtils.copyBytes(in,out,2048);
    }

    /**
     * 从hdfs下载文件到Windows时，copyToLocalFile方法必须使用前后加false和true 的，否则会报空指针
     * @throws IOException
     */
    @Test
    public void testCopyToLocalFile() throws IOException {
        Path src = new Path("/hdfsapi/test/aa.txt");
        Path dst = new Path("E:/");
        fileSystem.copyToLocalFile(false,src,dst,true);
    }

    @Test
    public void testListFiles() throws IOException {
        FileStatus[] statuses = fileSystem.listStatus(new Path("/hdfsapi/test/"));
        for (FileStatus file:statuses) {
            String isDir = file.isDirectory() ? "文件夹" : "文件";
            String permission = file.getPermission().toString();
            short replication = file.getReplication();
            long length = file.getLen();
            String path = file.getPath().toString();

            System.out.println(isDir + "\t" + permission + "\t" + replication + "\t" + length + "\t" + path);
        }
    }

    /**
     * 递归
     */
    @Test
    public void testListFilesR() throws IOException {
        RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(new Path("/hdfsapi/test/"),true);
        while (files.hasNext()){
            LocatedFileStatus file = files.next();
            String isDir = file.isDirectory() ? "文件夹" : "文件";
            String permission = file.getPermission().toString();
            short replication = file.getReplication();
            long length = file.getLen();
            String path = file.getPath().toString();

            System.out.println(isDir + "\t" + permission + "\t" + replication + "\t" + length + "\t" + path);
        }
    }

    @Test
    public void testFileBlockLocations() throws IOException {
        FileStatus fileStatus = fileSystem.getFileStatus(new Path("/hdfsapi/test/linux.tar.gz.test"));
        BlockLocation[] blockLocations = fileSystem.getFileBlockLocations(fileStatus,0,fileStatus.getLen());

        for (BlockLocation block:blockLocations) {
            for (String name :block.getNames()){
                System.out.println(name + "\t" + block.getOffset() + "\t" + block.getLength() + "\t" + block.getStorageIds());
            }
        }
    }

    @Test
    public void testDelete() throws IOException {
        boolean result = fileSystem.delete(new Path("/hdfsapi/test/linux.tar.gz.test"),true);
        System.out.println(result);
    }


}




















