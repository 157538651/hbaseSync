package sync.dao;



import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

public class HdfsDao {

    private static  String HDFS = "";
    
    private String hdfsPath;
    
    private Configuration conf;
    
    public HdfsDao(Configuration conf) {
        this(HDFS, conf);
    }

    public HdfsDao(String hdfs, Configuration conf) {
        this.hdfsPath = hdfs;
        this.conf = conf;
    }

  

    public void mkdirs(String folder) throws IOException {
        Path path = new Path(folder);
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        if (!fs.exists(path)) {
            fs.mkdirs(path);
            System.out.println("Create: " + folder);
        }
        fs.close();
    }
    /**
     *  判断路径是否存在
     * @param folder
     * @return 如果存在返回true,不存在返回false
     * @throws IOException
     */
    public boolean existspath(String folder) throws IOException {
        if(folder.length()<1)return false;
    	Path path = new Path(folder);
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        if (fs.exists(path)) {
        	fs.close();
        	return true;
        }else return false;
        
    }
    public void rmr(String folder) throws IOException {
        Path path = new Path(folder);
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        fs.deleteOnExit(path);
        System.out.println("Delete: " + folder);
        fs.close();
    }
    public List<Path> getDirFile(String folder) throws IOException{
           List<Path>  path_list = new ArrayList<Path>();
           Path path = new Path(folder);
           Configuration conf = new Configuration();
           FileSystem hdfs = FileSystem.get(URI.create(folder), conf);
           FileStatus[] list = hdfs.listStatus(path);
           Path[] listPath = FileUtil.stat2Paths(list);
           for(Path p : listPath){
            path_list.add(p);
          }
           return path_list;
    }
    public void rename(String src, String dst) throws IOException {
        Path name1 = new Path(src);
        Path name2 = new Path(dst);
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        fs.rename(name1, name2);
        System.out.println("Rename: from " + src + " to " + dst);
        fs.close();
    }

    public void ls(String folder) throws IOException {
        Path path = new Path(folder);
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        FileStatus[] list = fs.listStatus(path);
        System.out.println("ls: " + folder);
        System.out.println("==========================================================");
        for (FileStatus f : list) {
            System.out.printf("name: %s, folder: %s, size: %d\n", f.getPath(), f.isDir(), f.getLen());
        }
        System.out.println("==========================================================");
        fs.close();
    }

    public void createFile(String file, String content) throws IOException {
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        byte[] buff = content.getBytes();
        
        FSDataOutputStream os = null;
        try {
            os = fs.create(new Path(file));
            os.write(buff, 0, buff.length);
            
            System.out.println("Create: " + file);
        } finally {
            if (os != null)
                os.close();
        }
        fs.close();
    }
    public void createFileNoclose(String file, String content) throws IOException {
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        byte[] buff = content.getBytes();
        FSDataOutputStream os = null;
        try {
            os = fs.create(new Path(file));
            os.write(buff, 0, buff.length);
            
            //System.out.println("Create: " + file);
        } finally {
            /*if (os != null)
                os.close();*/
        }
        //fs.close();
    }

    public void copyFile(String local, String remote) throws IOException {
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        fs.copyFromLocalFile(new Path(local), new Path(remote));
        System.out.println("copy from: " + local + " to " + remote);
        fs.close();
    }

    public void download(String remote, String local) throws IOException {
        Path path = new Path(remote);
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        fs.copyToLocalFile(path, new Path(local));
        System.out.println("download: from" + remote + " to " + local);
        fs.close();
    }

    public void cat(String remoteFile) throws IOException {
        Path path = new Path(remoteFile);
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        FSDataInputStream fsdis = null;
        System.out.println("cat: " + remoteFile);
        try {
            fsdis = fs.open(path);
            IOUtils.copyBytes(fsdis, System.out, 4096, false);
        } finally {
            IOUtils.closeStream(fsdis);
            fs.close();
        }
    }

    /**
     * 移动
     * @author Chenpf
     * @param old_st原来存放的路径
     * @param new_st移动到的路径
     */
    public boolean moveFileName(String old_st, String new_st) {

        try {

            // 下载到服务器本地
            boolean down_flag = sendFromHdfs(old_st, "/home/hadoop/文档/temp");
            Configuration conf = new Configuration();
            FileSystem fs = null;

            // 删除源文件
            try {
                fs = FileSystem.get(URI.create(old_st), conf);
                Path hdfs_path = new Path(old_st);
                fs.delete(hdfs_path);
            } catch (IOException e) {
                e.printStackTrace();
            }

            // 从服务器本地传到新路径
            new_st = new_st + old_st.substring(old_st.lastIndexOf("/"));
            boolean uplod_flag = sendToHdfs1("/home/hadoop/文档/temp", new_st);

            if (down_flag && uplod_flag) {
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }
    /**
     * 下载,将hdfs文件下载到本地磁盘
     * @author Chenpf
     * @param localSrc1 本地的文件地址，即文件的路径
     * @param hdfsSrc1  存放在hdfs的文件地址
     */
    public boolean sendFromHdfs(String hdfsSrc1, String localSrc1) {

        Configuration conf = new Configuration();
        FileSystem fs = null;
        try {
            fs = FileSystem.get(URI.create(hdfsSrc1), conf);
            Path hdfs_path = new Path(hdfsSrc1);
            Path local_path = new Path(localSrc1);

            fs.copyToLocalFile(hdfs_path, local_path);

            return true;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }
    /**
     * 上传，将本地文件copy到hdfs系统中
     * @author Chenpf
     * @param localSrc 本地的文件地址，即文件的路径
     * @param hdfsSrc 存放在hdfs的文件地址
     */
    public boolean sendToHdfs1(String localSrc, String hdfsSrc) {
        InputStream in;
        try {
            in = new BufferedInputStream(new FileInputStream(localSrc));
            Configuration conf = new Configuration();// 得到配置对象
            FileSystem fs; // 文件系统
            try {
                fs = FileSystem.get(URI.create(hdfsSrc), conf);
                // 输出流，创建一个输出流
                OutputStream out = fs.create(new Path(hdfsSrc),
                        new Progressable() {
                            // 重写progress方法
                            public void progress() {
                                // System.out.println("上传完一个设定缓存区大小容量的文件！");
                            }
                        });
                // 连接两个流，形成通道，使输入流向输出流传输数据,
                IOUtils.copyBytes(in, out, 10240, true); // in为输入流对象，out为输出流对象，4096为缓冲区大小，true为上传后关闭流
                return true;
            } catch (IOException e) {
                e.printStackTrace();
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return false;
    }
    /**
     * @author Chenpf
     * @param conf
     * @throws Exception
     */
 // 获取给定目录下的所有子目录以及子文件
    public  void getAllChildFile(Configuration conf) throws Exception {
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("/user/hadoop");
        getFile(path, fs);
    }
    
    public  void getFile(Path path, FileSystem fs)throws Exception {
        FileStatus[] fileStatus = fs.listStatus(path);
        for (int i = 0; i < fileStatus.length; i++) {
            if (fileStatus[i].isDir()) {
                Path p = new Path(fileStatus[i].getPath().toString());
                getFile(p, fs);
            } else {
                System.out.println(fileStatus[i].getPath().toString());
            }
        }
    }
    
    
}

