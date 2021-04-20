package com.wgc.hdfsTest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;

import java.io.*;

/**
 * @author wanggc
 * @date 2019/08/29 星期四 15:34
 */
public class HdfsHelper {

    private FileSystem fs;

    public HdfsHelper() {
        fs = getFileSystem();
    }

    /**
     * Configuration是配置对象，conf可以理解为包含了所有配置信息的一个集合，可以认为是Map,
     * 在初始化的时候底层会加载一堆配置文件 core-site.xml;hdfs-site.xml;mapred-site.xml;yarn-site.xml
     * 如果需要项目代码自动加载配置文件中的信息，那么就必须把配置文件改成-default.xml或者-site.xml的名称，
     * 而且必须放置在src下,如果不叫这个名，或者不在src下，也需要加载这些配置文件中的参数，必须使用conf对象提供的方法手动加载.
     * 依次加载的参数信息的顺序是：
     * 1.加载core/hdfs/mapred/yarn-default.xml
     * 2.加载通过conf.addResource()加载的配置文件
     * 3.加载conf.set(name,value)
     */
    private Configuration getConfiguration() {
        Configuration conf = new Configuration();
        //    HA加Kerberos的配置
        //    1.HA模式的配置
        conf.set("fs.defaultFS", "hdfs://nameservice1");
        conf.set("dfs.nameservices", "nameservice1");
        conf.set("dfs.ha.namenodes.nameservice1", "nn1,nn2");
        conf.set("dfs.namenode.rpc-address.nameservice1.nn1", "bigdata014230:8020");
        conf.set("dfs.namenode.rpc-address.nameservice1.nn1", "bigdata014231:8020");
        conf.set("dfs.client.failover.proxy.provider.nameservice1", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem"); //防止报错 no FileSystem for scheme: hdfs...

        //   2. 配置Kerberos
        System.setProperty("java.security.krb5.conf", "E:\\IdeaProjects\\hdfsTest\\src\\main\\resources\\krb5.conf");
        conf.set("dfs.datanode.kerberos.principal", "hdfs/_HOST@MYCDH");
        conf.set("dfs.namenode.kerberos.principal", "hdfs/_HOST@MYCDH");
        conf.set("hadoop.security.authentication", "Kerberos");
        UserGroupInformation.setConfiguration(conf);
        try {
            UserGroupInformation.loginUserFromKeytab("eda@MYCDH", "E:\\IdeaProjects\\hdfsTest\\src\\main\\resources\\eda.keytab");
        } catch (IOException e) {
            e.printStackTrace();
        }

        return conf;
    }

    /**
     * 获取文件系统
     * 本地文件系统为LocalFileSystem，URL形式:    file:///c:myProgram
     * HDFS文件系统为DistributedFileSystem，URL形式:    fs.defaultFS=hdfs://hadoop01:9000
     */
    public FileSystem getFileSystem() {
        Configuration conf = getConfiguration();
        FileSystem fs = null;
        try {
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println(fs);
        return fs;
    }

    //下面开始实现hdfs api功能

    /**
     * 1、获取目录下的文件
     *
     * @param remotePath HDFS文件路径
     * @param recursive  是否级联（该文件夹下面如果还有子文件 要不要看,注意没有 子文件夹!!）
     */
    public void listHdfsFiles(String remotePath, boolean recursive) {
        if (fs == null)
            return;
        try {
            RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(new Path(remotePath), recursive);
            while (iterator.hasNext()) {
                LocatedFileStatus fileStatus = iterator.next();
                //    文件的存储路径，以hdfs://开头的全路径 ==> hdfs://hadoop01:9000/a/gg.txt
                System.out.println("file path ====" + fileStatus.getPath());
                //    文件名
                System.out.println("file name ====" + fileStatus.getPath().getName());
                //    文件changdu
                System.out.println("file length ====" + fileStatus.getLen());
                //    文件所有者
                System.out.println("file owner ====" + fileStatus.getOwner());
                //    分组信息
                System.out.println("file group ====" + fileStatus.getGroup());
                //    文件权限信息
                System.out.println("file permission ====" + fileStatus.getPermission());
                //    文件副本数
                System.out.println("file blocks ====" + fileStatus.getReplication());
                //    块大小
                System.out.println("file bloack size ====" + fileStatus.getBlockSize());
                //   块位子信息
                BlockLocation[] blockLocations = fileStatus.getBlockLocations();
                //    块的数量
                System.out.println("file block nums ====" + blockLocations.length);
                for (BlockLocation bl : blockLocations) {
                    String[] hosts = bl.getHosts();
                        for (String host : hosts) {
                            System.out.println("block host ====" + host);
                        }
                        //    块的一个逻辑路径
                        bl.getTopologyPaths();
                    }
                }
            } catch (IOException e) {
            e.printStackTrace();
        }
        }

    /**
     * 1、获取目录下的文件的另一种方法
     * 此方法与listFiles不同,不支持传true或false,即不能级联，如果想实现级联就采用递归的方式
     * @param remotePath HDFS文件路径
     */
    public void listHdfsFile2(String remotePath){
        if (fs == null){
            return;
        }
        try {
            FileStatus[]  listHdfsFile2= fs.listStatus(new Path(remotePath));
            for (FileStatus fss:listHdfsFile2){
            //    is directory?
                boolean directory = fss.isDirectory();
            //    is file?
                boolean file = fss.isFile();

                String name = fss.getPath().getName();

                if (file){
                    System.out.println(name + ":文件");
                } else {
                    System.out.println(name+ ":文件夹");
                }
            }
        } catch (IOException e){
            e.printStackTrace();
        }
    }

    /**
     * 2.删除空文件夹或空文件
     * @param path
     */
    public void deleteEmptyDirAndFile(Path path){
        if (fs == null){
            return;
        }
        try{
            FileStatus[] listStatus = fs.listStatus(path);
            if (listStatus.length == 0){
            //    删除空文件夹
                fs.delete(path, true);
                return;
            }

            RemoteIterator<LocatedFileStatus> iterator = fs.listLocatedStatus(path);
            while (iterator.hasNext()){
                LocatedFileStatus next = iterator.next();
                Path currentPath = next.getPath();
                Path parentPath = next.getPath().getParent();

                if (next.isDirectory()){
                //    如果时空的文件夹就删除
                    if (fs.listStatus(currentPath).length == 0) {
                        fs.delete(currentPath, true);
                    } else {
                    //    不是则继续遍历
                        if (fs.exists(currentPath)){
                            deleteEmptyDirAndFile(currentPath);
                        }
                    }
                } else {
                //    获取文件长度
                    long fileLength = next.getLen();
                //    当文件是空文件时，删除
                    if (fileLength == 0){
                        fs.delete(currentPath, true);
                    }
                }
            //    当空文件夹或者空文件删除时，有可能导致父文件夹为空文件夹，
            //    所以每次删除一个空文件或者空文件的时候都需要判断一下，如果真是如此，那么就需要把该文件夹也删除掉
                int length = fs.listStatus(parentPath).length;
                if (length == 0){
                    fs.delete(parentPath, true);
                }
            }
        } catch (IOException e){
            e.printStackTrace();
        }
    }

    /**
     * 3.创建文件夹
     * @param remotePath HDFS文件路径
     * @return 是否创建成功
     */
    public boolean mkdir(String remotePath){
        if (fs == null){
            return false;
        }
        boolean success = false;
        try{
            success = fs.mkdirs(new Path(remotePath));
        } catch (IOException e){
            e.printStackTrace();
        }
        return success;
    }

    /**
     * 4、写入文件
     * @param remotePath HDFS文件路径
     * @param content 内容
     * @return 是否写入成功
     */
    public boolean writeToFile(String remotePath,String content){
        if (fs == null){
            return false;
        }
        try {
            FSDataOutputStream fout = fs.create(new Path(remotePath));
            fout.writeUTF(content);
            fout.close();
        } catch (IOException e){
            e.printStackTrace();
            return false;
        }
        return true;
    }
    /**
     * 5、读取文件数据
     * @param remotePath HDFS文件路径
     * @return 读取的结果数据
     */
    public String readFromFile(String remotePath){
        String result = null;
        if(fs == null) return null;
        try {

            FSDataInputStream in = fs.open(new Path(remotePath));
            result = in.readUTF();
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
     * 6、重命名文件
     * @param oldName 旧文件路径
     * @param newName 新文件路径
     * @return 是否重命名成功
     */
    public boolean renameFile(String oldName,String newName){
        if (fs == null){
            return false;
        }
        Path oldname = new Path(oldName);
        Path newname = new Path(newName);
        boolean rename = false;
        try {
            rename = fs.rename(oldname, newname);
        } catch (IOException e){
            e.printStackTrace();
        }

        return rename;
    }

    /**
     * 7、删除目录和文件
     * @param remotePath HDFS文件路径
     * @return 是否删除成功
     */

    public boolean deleteFile(String remotePath){
        if (fs == null){
            return false;
        }
        boolean success = false;
        try {
            success = fs.delete(new Path(remotePath), true);
        } catch (IOException e){
            e.printStackTrace();
        }
        return success;
    }

    /**
     * 8、检查文件是否存在
     * @param remotePath HDFS文件路径
     * @return 是否存在
     */
    public boolean existFile(String remotePath){
        if (fs == null) {
            return false;
        }
        boolean exist = false;
        try {
            exist = fs.exists(new Path(remotePath));
        } catch (IOException e){
            e.printStackTrace();
        }
        return exist;
        }

    /**
     * 9、上传本地文件到HDFS，底层就是采用流的方式
     * @param localPath 本地文件路径
     * @param remotePath HDFS文件路径
     * @return 是否上传成功
     */
    public boolean putFileToHdfs(String localPath,String remotePath){
        if (fs == null){
            return false;
        }
        try {
            fs.copyFromLocalFile(new Path(localPath),new Path(remotePath));
        } catch (IOException e){
            e.printStackTrace();
        }
        return true;
    }

    /**
     * 10、从HDFS下载文件，底层就是采用流的方式
     * @param remotePath HDFS文件路径
     * @param localPath 本地路径
     * @return 是否下载成功
     */
    public boolean getHdfsFileToLocal(String remotePath,String localPath){
        if (fs == null){
            return false;
        }
        try {
            fs.copyFromLocalFile(new Path(remotePath),new Path(localPath));
        } catch (IOException e){
            e.printStackTrace();
            return false;
        }

        return true;
    }

    /**
     * IOUtils 类
     * 读取hdfs文件
     * @param remotePath HDFS文件路径
     * @return 读取的结果数据
     */
    public void readFromFile2(String remotePath){
        //String result = null;
        if (fs == null){
            return ;
        }
        try {
            FSDataInputStream fin = fs.open(new Path(remotePath));
            IOUtils.copyBytes(fin, System.out, 4096, false);
            IOUtils.closeStream(fin);
        } catch (IOException e){
            e.printStackTrace();
        }
    }

    /**
     * IOUtils 类
     * 上传本地文件到HDFS
     * @param localPath 本地文件路径
     * @param hdfsPath HDFS文件路径
     * @return 是否上传成功
     */
    /**
     * File对象上传到hdfs
     */
    public void putFileToHdfs2(String localPath, String hdfsPath)  {
        InputStream inputStream = null;
        if (fs == null) return;
        try {
            File file = new File(localPath);
            inputStream = new BufferedInputStream(new FileInputStream(file));
            final float fileSize = localPath.length()/65536;
            FSDataOutputStream fsDataOutputStream = fs.create(new Path(hdfsPath), new Progressable() {
                long fileCount = 0;
                public void progress() {
                    fileCount ++;
                    System.out.println("总进度：" + (fileCount/fileSize)*100 + " %");
                }
            });
            IOUtils.copyBytes(inputStream, fsDataOutputStream, 4096, false);
            fsDataOutputStream.close();
            System.out.println("create file in hdfs: " + hdfsPath);
        } catch (IOException e){
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(inputStream);
        }
        System.out.println("put file to hdfs success via IOUtils ==>" + hdfsPath);
    }

    /**
     * FileUtil 工具类
     * 这个方法会将一个数组的status转化为数组的path对象。
     * 测试文件上传hdfs
     * */
    public void putFileToHdfs3(String localPath,String hdfsPath){
        if (fs == null) return;
        try {
            File file = new File(localPath);
            FileUtil.copy(file,fs,new Path(hdfsPath),false,getConfiguration());
        } catch (IOException e){
            e.printStackTrace();
        }
        System.out.println("put file to hdfs success via FileUtil ==>"+ hdfsPath);
    }

    /**
     * FileUtil 合并文件
     * 因为hadoop中对小文件的处理是非常不擅长的，
     * 因此我们可能需要对小文件进行合并。FileUtil中提供了一个方法copyMerge方法
     * */
    public void mergeHdfsFile(String srcDir,String desDir){
        if (fs == null) return;
        try {
            FileUtil.copyMerge(fs, new Path(srcDir), fs, new Path(desDir), false, getConfiguration(), "ok");
        } catch (IOException e){
            e.printStackTrace();
        }
        System.out.println("merge hdfs file success ==>"+ desDir);
    }


    /**
     * 关闭FileSystem
     */
    public void closeFileSystem(){
        if (fs != null){
            try {
                fs.close();
            } catch (IOException e){
                e.printStackTrace();
            }
        }

        System.out.println("filesystem closed success");
    }


}





/*
        //1、上传文件到hdfs，hadoop fs -put hdfsUpload.txt /user/eda/
        fileSystem.copyFromLocalFile(new Path("E:\\hdfsUpload.txt"), new Path("hdfs://nameservice1/user/eda/"));
        System.out.println("----put hdfs file success!----");
        //2、创建hdfs目录,hadoop fs -mkdir /user/eda/hdfsApiTest
        fileSystem.mkdirs(new Path("hdfs://nameservice1/user/eda/hdfsApiTest"));
        System.out.println("----mkdir hdfs directory success!----");
        //3、查看hdfs文件内容,hadoop fs -cat /user/eda/hdfsUpload.txt
        FSDataInputStream fsDataInputStream = fileSystem.open(new Path("hdfs://nameservice1/user/eda/hdfsUpload.txt"));
        IOUtils.copyBytes(fsDataInputStream,System.out,1024);
        fsDataInputStream.close();
        System.out.println("----cat hdfs file success!----");

        //4、hdfs文件重命名
        fileSystem.rename(new Path("hdfs://nameservice1/user/eda/hdfsUpload.txt"), new Path("hdfs://nameservice1/user/eda/hdfsUpload_rename.txt"));
        System.out.println("---rename filename success!----");
    }
*/
