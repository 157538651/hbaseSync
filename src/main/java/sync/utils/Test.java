package sync.utils;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Test {
	public static final String BASEPATH = "d://";
	public static final String SUFFIX = ".csv";
	public static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	public static void main(String[] args) throws IOException {
		String newLine = System.getProperty("line.separator");
		
		String fileName = sdf.format(new Date())+SUFFIX;
		
		FileUtils.addContent(BASEPATH+fileName, "第7月8号次写数据!"+newLine);
	}
	
}
