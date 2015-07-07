package sync.main;

import java.io.IOException;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.PutSortReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sync.conf.ConfClz;
import sync.conf.ReadConfig;
import sync.dao.HdfsDao;
import sync.mapper.ExportHBaseMapper;
import sync.mapper.ReadFileMpper;
import sync.reduce.ReadFileReduce;

public class RunMain {
	
	private static CommandLine cl = null; 
	    
	public static final String cf = "bd";
	    
	private static ConfClz confClz = new ConfClz();
	
	private final static String TEMPDIR = "/tempDir";
	
	private final static String HFILE_TEMPDIR = "/hfile_tempDir";
	
	private static final Logger LOG = LoggerFactory.getLogger(RunMain.class);
	
	
	 /**
     * 计数器 用于计数各种异常数据
     */
    enum Counter {
        LINESKIP, // 出错的行
    }
    
    /**
     * 主函数
     * @param args
     */
	public static void main(String[] args) {
		try {
				getCommandParam(args);
				getConf();
			
				Configuration conf = HBaseConfiguration.create();
				Properties filepath = ReadConfig.filepath;
			
		        String host = filepath.getProperty("host");
		        String headers = filepath.getProperty("headers");
		        String inputPath = filepath.getProperty("inputPath");
		        
		        confClz.setInputPath(inputPath);
		        
				conf.set("hbase.zookeeper.quorum", host);
				conf.set("hbase.nameserver.address", host);
		
				runReadFileJob(conf);
				
				runCreateHfile(conf,headers);
				
				HTable table = new HTable(conf, confClz.getTargetTable());
				LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
				
			    loader.doBulkLoad(new Path(HFILE_TEMPDIR), table);
		        
		} catch (Exception e) {
			e.printStackTrace();
			LOG.error(e.getMessage());
		}
	}
	
	/**
	 * @Description 删除零时目录
	 * @param conf
	 * @throws IOException
	 */
	public static void rmr(Configuration conf) throws IOException{
		 HdfsDao dao = new HdfsDao(conf);
		 if(dao.existspath(TEMPDIR)){
         	dao.rmr(TEMPDIR);
         }
         if(dao.existspath(HFILE_TEMPDIR)){
        	 dao.rmr(HFILE_TEMPDIR); 
         }
	}
	/**
	 * 
	 * @Description 读取hdfs文件
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
    public static void runReadFileJob(Configuration conf) throws IOException, 
    								  ClassNotFoundException, InterruptedException{
    	DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date start = new Date();
    	Job job = new Job(conf, "read hdfs file");
    	job.setJarByClass(RunMain.class);
    	job.setMapOutputKeyClass(Text.class);
    	job.setMapOutputValueClass(Text.class);
    	job.setMapperClass(ReadFileMpper.class);
    	
    	job.setReducerClass(ReadFileReduce.class);
    	
    	job.setOutputKeyClass(Text.class);
    	job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPaths(job,confClz.getInputPath());
        FileOutputFormat.setOutputPath(job, new Path(TEMPDIR));
        
        job.waitForCompletion(true); 
        sysoinfo(formatter,start,job);
    }

    /**
     * @Description  任务:生成hfile
     * @param conf
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public static void runCreateHfile(Configuration conf , String headers) throws IOException, 
    				                 ClassNotFoundException, InterruptedException{
        DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date start = new Date();
        
        Job job = new Job(conf, "create hfile");
        
        job.getConfiguration().set("headers", headers);
        job.setJarByClass(RunMain.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        
        FileInputFormat.setInputPaths(job, new Path(TEMPDIR));
        
        job.setMapperClass(ExportHBaseMapper.class);
        
        HTable table = new HTable(conf, confClz.getTargetTable());
        job.setReducerClass(PutSortReducer.class);
        
        Path outputDir = new Path(HFILE_TEMPDIR);
        FileOutputFormat.setOutputPath(job, outputDir);
        
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);
        
        HFileOutputFormat2.configureIncrementalLoad(job, table);
        TableMapReduceUtil.addDependencyJars(job);
        
        job.waitForCompletion(true);
        sysoinfo(formatter,start,job);
    }
    
    /**
     * @Description 打印日志
     * @param formatter
     * @param start
     * @param job
     * @throws IOException
     */
    private static void sysoinfo(DateFormat formatter, Date start, Job job)
            throws IOException {
        LOG.info("任务名称：" + job.getJobName());
        LOG.info("任务成功：" + (job.isSuccessful() ? "是" : "否"));
        
        LOG.info("输入行数："+ job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter",
                                "MAP_INPUT_RECORDS").getValue());
        
        LOG.info("输出行数："+ job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter",
                                "REDUCE_OUTPUT_RECORDS").getValue());
        
        LOG.info("跳过的行："+ job.getCounters().findCounter(Counter.LINESKIP).getValue());
        
        // 输出任务耗时
        Date end = new Date();
        float time = (float) ((end.getTime() - start.getTime()) / 60000.0);
        
        LOG.info("任务开始：" + formatter.format(start));
        LOG.info("任务结束：" + formatter.format(end));
        
        DecimalFormat df2 = new DecimalFormat("#.###");
        
        LOG.info("任务耗时：" + df2.format(time) + " 分钟");
    }

	/**
     * 
     * @MethodName: getCommandParam
     * @Description: 从命令行获取参数
     * @param args
     * @return void 
     * @throws
     */
    private static void getCommandParam(String[] args) {
        Options opt = new Options();
        opt.addOption("tb", true, "source hbase table");
        opt.addOption("hfilePath", "hfile path", true, "hfile output hdfs path");
        
        String formatStr = "sh hadoop [this jar path][-tb]";
        HelpFormatter formatter = new HelpFormatter();
        
        CommandLineParser parser = new PosixParser();
        try {
            cl = parser.parse(opt, args); 
        } catch (Exception e) {
            formatter.printHelp(formatStr, opt);
            System.exit(1);
            LOG.error("", e);
        }
    }
    
    /**
     * 
     * @MethodName: getConf
     * @Description: 加载配置文件
     * @return void 
     * @throws
     */
    private static void getConf() {
        //读取hbase表名
        String targetTable = cl.getOptionValue("tb");
        if (StringUtils.isEmpty(targetTable)) {
            LOG.error("没有指定HBASE表名");
            System.exit(1);
        }
        confClz.setTargetTable(targetTable);
       
    }
    
}
