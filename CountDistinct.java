package extend.udf;

import java.util.Arrays;
import java.util.HashSet;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

@Description(name = "countdist", value = "_FUNC_(param) - Returns remove repeat data's count")
public class CountDistinct extends UDF {
	
	public int evaluate(String values) {
		if (values == null) return 0;
		HashSet<String> hs = new HashSet<String>(Arrays.asList(values.split(",")));
		if (hs.contains("")) {
			hs.remove("");
		}
		if (hs.contains("NULL")) {
			hs.remove("NULL");
		}
		return hs.size();
	}
	
}
