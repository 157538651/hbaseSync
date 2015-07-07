package sync.mapper;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.directory.api.util.Strings;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sync.main.RunMain;

public class ReadFileMpper extends Mapper<Object, Text, Text, Text>{
	
	private final static String SPLIT = "\\|";
	
	private final static String CHARCTER_L = "&";
	
	private final static String CHARCTER_C = "|";
	
	private static final Logger LOG = LoggerFactory.getLogger(RunMain.class);
	
	Text k;
    Text v;
    protected void setup(Context context)
            throws IOException, InterruptedException {
        k = new Text("");
        v = new Text("");
     }
    @Override
    protected void map(Object key, Text value,Context context){
    	try{
	    	String[] splitLine = splitLine(value.toString(),SPLIT);
	    	String currentTimeMillis = System.currentTimeMillis()+"";
	    	if(null != splitLine && splitLine.length>6){
	    		StringBuilder header = new StringBuilder();
	    		for(int i=0;i<6;i++){
	    			header.append(splitLine[i]).append(CHARCTER_C);
	    		}
	            k.set(header.toString()+currentTimeMillis);
	            v.set(header+CHARCTER_L+splitLine[6]);
	            
	            if(null != k && null !=v){
	        		context.write(k, v);
	        	}
	    	}
    	}catch(Exception e){
    		LOG.error(e.getMessage());
    	}
    }
    
    
    @Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		super.cleanup(context);
		k = null;
		v = null;
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
