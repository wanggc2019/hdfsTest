package com.wgc.hdfsTest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author wanggc
 * @date 2019/08/30 星期五 15:19
 */
public class HdfsList {

    //1、列出hdfs路径下的文件
    public static List<String> getHdfsFiles(String filePath) throws IOException {
    //public static List<String> getHdfsFiles(String filePath, String typeName) throws IOException {
        //IHdfsConfigDao hdfsDao = new HdfsConfigDao();
        //List<HdfsConfig> hdfs =  hdfsDao.selectList(typeName);
        //创建configuration对象
        Configuration config = new Configuration();
        System.setProperty("java.security.krb5.conf", "E:\\IdeaProjects\\hdfsTest\\src\\main\\resources\\krb5.conf");
        config.set("fs.defaultFS", "hdfs://nameservice1");
        config.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        config.set("dfs.nameservices", "nameservice1");
        config.set("dfs.client.failover.proxy.provider.nameservice1", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        config.set("dfs.ha.namenodes.nameservice1", "nn1,nn2");
		config.set("dfs.namenode.rpc-address.nameservice1.nn1",	"bigdata014230:8020");
		config.set("dfs.namenode.rpc-address.nameservice1.nn2",	"bigdata014231:8020");
		config.set("dfs.datanode.kerberos.principal", "hdfs/_HOST@MYCDH");
		config.set("dfs.namenode.kerberos.principal", "hdfs/_HOST@MYCDH");
        //config.set("dfs.namenode.rpc-address.nameservice1.nn1",	hdfs.get(0).getNn1());
        //config.set("dfs.namenode.rpc-address.nameservice1.nn2",	hdfs.get(0).getNn2());
        config.set("hadoop.security.authentication", "Kerberos");
        //config.set("dfs.datanode.kerberos.principal", hdfs.get(0).getDatanodeKbPcp());
        //config.set("dfs.namenode.kerberos.principal", hdfs.get(0).getNamenodeKbPcp());
        UserGroupInformation.setConfiguration(config);
		UserGroupInformation.loginUserFromKeytab("eda@MYCDH", "E:\\IdeaProjects\\hdfsTest\\src\\main\\resources\\eda.keytab");
        //UserGroupInformation.loginUserFromKeytab(hdfs.get(0).getKeyTabUser(), hdfs.get(0).getKeyTabPswd());
        System.out.println("----hadoop.security.authentication=" + config.get("hadoop.security.authentication")+"----");
        //创建FileSystem对象
        FileSystem fs = FileSystem.get(config);
        List<String> files = new ArrayList<String>();
//	    Path s_path = new Path("hdfs://192.168.128.150:8020/user/hch/");
        Path s_path = new Path(filePath);
        System.out.println("s_path="+s_path);
        System.out.println("files start");
	    FileStatus[] fileStatuses = fs.listStatus(s_path);
	    System.out.println("fileStatuses.length================"+fileStatuses.length);
        if(fs.exists(s_path)){
	    	int n=0;
            for(FileStatus status:fs.listStatus(s_path)){
	    	    System.out.println("files["+n+"]================"+status.getPath().toString());
                files.add(status.getPath().toString());
	            n++;
            }
        }
        System.out.println("files end");
        fs.close();
        return files;
    }

    public static void main(String[] args) throws IOException{
        HdfsList.getHdfsFiles("hdfs://nameservice1/user/hive/warehouse/eda.db/");
    }

}
