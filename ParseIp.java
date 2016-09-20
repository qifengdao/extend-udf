package extend.udf;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

@Description(name = "parseip", value = "_FUNC_(param) - Returns the  IP address to area address,IP library is  /hive/ip.txt at hdfs")
public  class ParseIp extends UDF{
    
	public final static String FILE_PATH = "/hive/ip.txt";
	static int count;
	static List<long[]> areas = new ArrayList<long[]>();
	static List<String[]> area_name = new ArrayList<String[]>();
	
	static {
		BufferedReader br = null;
		try {
			Configuration conf = new Configuration();
		    FileSystem fs = FileSystem.get(conf);
	        FSDataInputStream in = fs.open(new Path(FILE_PATH));
	        br = new BufferedReader(new InputStreamReader(in));
			String data = br.readLine();
			while (data != null) {
				String[] columns = data.split("\\s+");
				if (columns.length >= 4) {
					String[] ips = columns[0].split("\\.");
					long start = Long.parseLong(ips[0]) * 256 * 256 * 256
							+ Long.parseLong(ips[1]) * 256 * 256
							+ Long.parseLong(ips[2]) * 256
							+ Long.parseLong(ips[3]);
					ips = columns[1].split("\\.");
					long end = Long.parseLong(ips[0]) * 256 * 256 * 256
							+ Long.parseLong(ips[1]) * 256 * 256
							+ Long.parseLong(ips[2]) * 256
							+ Long.parseLong(ips[3]);
					areas.add(new long[] { start, end });
					area_name.add(new String[] { columns[2], columns[3] });
				} 
				//else {
				//	System.out.println(data);
			//	}
				data = br.readLine();
			}
			count = areas.size();
		} 
        catch (Exception e) {
			e.printStackTrace();
		} finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e1) {
                }
            }
        }
	}


	public String evaluate(String ip) {
		int start = 1;
		int end = count;
		String[] ips = ip.split(":")[0].split("\\.");
		if (ips.length != 4) {
			return  "δ֪,δ֪";
		}
		try {
			long lip = Long.parseLong(ips[0]) * 256 * 256 * 256
					+ Long.parseLong(ips[1]) * 256 * 256
					+ Long.parseLong(ips[2]) * 256 + Long.parseLong(ips[3]);
			while (start <= end && start >= 1 && end <= count) {
				int middle = (start + end) / 2;
				if (lip >= areas.get(middle)[0] && lip <= areas.get(middle)[1]) {
					return area_name.get(middle)[0]+","+area_name.get(middle)[1];
				}
				if (lip > areas.get(middle)[0]) {
					start = middle + 1;
				} else {
					end = middle - 1;
				}
			}
		} catch (Exception e) {
			return "δ֪,δ֪";
		}
		return "δ֪,δ֪";
	}

}
