package sync.utils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class FileUtils {
	
	 /**  
     * 追加文件：使用FileWriter  
     * @param fileName  
     * @param content  
     */  
    public static void writerFile(String fileName, String content) { 
    	FileWriter writer = null;
        try {   
            // 打开一个写文件器，构造函数中的第二个参数true表示以追加形式写文件   
            writer = new FileWriter(fileName, true);   
            writer.write(content);
        } catch (IOException e) {   
            e.printStackTrace();   
        } finally {   
            try {   
            	if(writer != null){
            		writer.close();   
            	}
            } catch (IOException e) {   
                e.printStackTrace();   
            }   
        } 
    }   
    
    public static void addContent(String fileName, String content) throws IOException{
		File file = new File(fileName);
		if(!file.exists()){
			file.createNewFile();
		}
		writerFile(fileName,content);
    }
}
