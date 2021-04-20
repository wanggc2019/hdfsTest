package com.wgc.hdfsTest;

/**
 * @author wanggc
 * @date 2019/09/02 星期一 14:18
 */
public class HdfsMain {
    public static void main(String[] args) {
        HdfsHelper hdfsHelper = new HdfsHelper();
        // 创建文件夹
        //boolean mkdir = hdfsHelper.mkdir("/user/eda/hdfsApiTest/test");
        //System.out.println("/user/eda/hdfsApiTest/test" + "create success ==>" + mkdir);
        //上传文件到hdfs
        //boolean putFileToHdfs = hdfsHelper.putFileToHdfs("E:\\hdfsUpload.txt","/user/eda/hdfsApiTest/test");
        //System.out.println("uplaod files to hdfs success ==>" + putFileToHdfs);
        //列出hdfs目录下文件
        hdfsHelper.listHdfsFiles("/user/eda/hdfsApiTest/test",false);
        //hdfsHelper.listHdfsFile2("/user/eda/hdfsApiTest/test");
        //写入hdfs文件
        //boolean write = hdfsHelper.writeToFile("/user/eda/hdfsApiTest/test/wgc.txt","my name is shuaibi\n");
        //System.out.println("write to hdfs success ==>" + write);
        //读取hdfs文件
        //String data = hdfsHelper.readFromFile("/user/eda/hdfsApiTest/test/wgc.txt");
        //System.out.println("read hdfs file success ==>" + data);
        //重命名hdfs文件
        //boolean renameFile = hdfsHelper.renameFile("/user/eda/hdfsApiTest/test/wgc.txt","/user/eda/hdfsApiTest/test/wgc_rename.txt");
        //System.out.println("rename hdfs file success ==>"+ renameFile);
        //get hdfs文件到本地
        //boolean loacl = hdfsHelper.getHdfsFileToLocal("/user/eda/hdfsApiTest/test/test.txt","E:\\");
        //System.out.println("get hdfs file success ==>" + loacl);
        //删除空的hdfs文件（夹）
        //hdfsHelper.deleteEmptyDirAndFile(new Path("/user/eda/hdfsApiTest/test/"));
        //删除指定得hdfs文件
        //boolean delete = hdfsHelper.deleteFile("/user/eda/hdfsApiTest/test/test_rename.txt");
        //System.out.println("delete hdfs file sucdess ==>" + delete);
        //IOUtils
        //IOUtils 上传文件
        //hdfsHelper.putFileToHdfs2("E:\\hdfsUpload.txt","/user/eda/hdfsApiTest/test/hdfsUpload.txt");
        //FileUtil 上传文件
        //hdfsHelper.putFileToHdfs3("E:\\hdfsUpload.txt","/user/eda/hdfsApiTest/test/hdfsUpload.txt");
        //hdfsHelper.mergeHdfsFile("/user/eda/hdfsApiTest/test/mergesrc","/user/eda/hdfsApiTest/test/mergetest");

        //关闭FileSysytem
        hdfsHelper.closeFileSystem();

    }
}

