package sync.conf;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * 
 * @author chen
 * @Description 读取配置文件
 * @date 2015年06月30日 下午09:49:16
 */
public class ReadConfig {
	public static Properties filepath;
	public static String confpath = "resources/config.properties";

	static {
		InputStream in = ReadConfig.class.getClassLoader().getResourceAsStream(
				confpath);// -------备注1
		filepath = new Properties();
		try {
			filepath.load(in);// 将输入流加载到配置对象,以使配置对象可以读取config.propertis信息
		} catch (IOException e) {
			System.err.println("初始化失败");
		}
	}
}
