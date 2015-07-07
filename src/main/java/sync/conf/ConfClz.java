package sync.conf;


/**
 * 
 * @author chen
 * @Description: 配置参数Bean
 * @date 2015年06月30日 下午10:59:16
 *
 */
public class ConfClz { 
	//目标表名
	private String targetTable = "";
	
	//列
	private String familyCol = "";
 
	//输入路径
	private String inputPath = "";
	
	//输出路径
	private String outputPath = "";

	//hfile路径
	private String hfilePath = "";

	public String getTargetTable() {
		return targetTable;
	}

	public void setTargetTable(String targetTable) {
		this.targetTable = targetTable;
	}

	public String getFamilyCol() {
		return familyCol;
	}

	public void setFamilyCol(String familyCol) {
		this.familyCol = familyCol;
	}

	public String getInputPath() {
		return inputPath;
	}

	public void setInputPath(String inputPath) {
		this.inputPath = inputPath;
	}

	public String getHfilePath() {
		return hfilePath;
	}

	public void setHfilePath(String hfilePath) {
		this.hfilePath = hfilePath;
	}

	public String getOutputPath() {
		return outputPath;
	}

	public void setOutputPath(String outputPath) {
		this.outputPath = outputPath;
	}
	
	 
}
