package sync.reduce;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * @author chen
 * @Description: 配置参数Bean
 * @date 2015年06月30日 下午14:59:16
 */
public class ReadFileReduce extends Reducer<Text, Text, Text, Text>{
	Text k ;
    Text v ;
    StringBuilder sb ;
	private final static String CHARCTER_L = "_";
	
    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {
        k = new Text("");
        v = new Text("");
        sb = new StringBuilder();
    }
    
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            			 throws IOException, InterruptedException {
    	
    	sb.setLength(0);
    	for (Text val : values) {
    		 sb.append(val).append(CHARCTER_L);
    	}
    	
    	k.set(key);
    	v.set(sb.toString());
    	
    	if(null != k && null !=v){
    		context.write(k, v);
    	}
    }
}
