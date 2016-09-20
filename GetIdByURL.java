package extend.udf;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

@SuppressWarnings("unchecked")
@Description(name = "getid4url", value = "_FUNC_(param) - Parse url to id,mapping txt  is  /hive/channel.txt at hdfs")
public class GetIdByURL extends UDF {

	public static Map<String,String> channelidMap;
	public final static String FILE_PATH = "/hive/channel.txt";
	
	static {
	    channelidMap = new TreeMap<String,String>(new MyCompartor());
	    FileSystem fs = null;
		Configuration conf = new Configuration();
		try {
			fs = FileSystem.get(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
		Path fpath = new Path(FILE_PATH);  
	    readhdfs(fpath, fs);
	}
	
	public static void readhdfs(Path Path, FileSystem fs) {
		BufferedReader br = null;
		FSDataInputStream in;
		try {
			in = fs.open(Path);
			 br = new BufferedReader(new InputStreamReader(in));
	         String str = null;
	         while ((str = br.readLine()) != null) {
	                channelidMap.put(str.split(",")[1], str.split(",")[0]);
	       }
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
		    try {
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}  
		}
	}
	
	public String evaluate(String url) {
		String result = "0";
		if(null == url || "".equals(url) || "NULL".equalsIgnoreCase(url)) return result;
		for(String k : channelidMap.keySet()) {
			if((url.trim().toLowerCase()).startsWith(k)) {
				result = channelidMap.get(k);
				break;
			}
		}
		return result;
	}
}
