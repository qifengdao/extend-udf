package extend.udf;

import java.util.Comparator;

@SuppressWarnings("rawtypes")
class MyCompartor implements Comparator {

	public int compare(Object o1, Object o2) {
		String sdto1 = (String) o1;
		String sdto2 = (String) o2;
		int num = new Integer(sdto2.length()).compareTo(new Integer(sdto1
				.length()));
		if (num == 0) {
			return sdto2.compareTo(sdto1);
		}
		return num;
	}
}