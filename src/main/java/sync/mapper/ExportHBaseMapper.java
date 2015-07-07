package sync.mapper;

import java.io.IOException;
import org.apache.commons.lang.StringUtils;
import org.apache.directory.api.util.Strings;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ExportHBaseMapper extends Mapper<Text, Text, ImmutableBytesWritable, Put> {
   String[] vals;
   byte[] rowKeyVal ;
   private final static String SPLIT_L = "&";
   
   private final static String FAMILY = "info";
   
   
   String [] headers;
   
   @Override
   protected void setup(Context context) throws IOException,
           InterruptedException {
       headers = context.getConfiguration().get("headers").split("\\|", -1);
   }

   
   @Override
   protected void map(Text key, Text value,Context context)
           throws IOException, InterruptedException {
     //分解行
       vals = splitLine(value.toString(), SPLIT_L);
       //rowkey 
       rowKeyVal = key.copyBytes();
       
       final Put put = new Put(rowKeyVal);
       for(int i=0;i<headers.length;i++){
           put.add(Bytes.toBytes(FAMILY), Bytes.toBytes(headers[i]),
                   Bytes.toBytes(vals[i]));
       }
       ImmutableBytesWritable immutableBytesWritable = new ImmutableBytesWritable();
       immutableBytesWritable.set(rowKeyVal);
       context.write(immutableBytesWritable, put);
   }
   
   /**
    * 分解行
    * @param lineValue
    * @param split
    * @return
    */
   private String[] splitLine(String lineValue, String split) {
       String line = Strings.trim(lineValue.toString());
       String[] vals = null;
       if (!StringUtils.isEmpty(split)) {
    	   vals = line.split(split, -1);
       }
       return vals;
   }
}
