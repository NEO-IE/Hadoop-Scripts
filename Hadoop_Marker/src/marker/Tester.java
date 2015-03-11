package marker;

import java.util.regex.Pattern;

import org.apache.commons.lang.math.NumberUtils;


public class Tester {
	public static void main(String args[]) {
		String num = "+1,23";
		Pattern re = Pattern.compile("^[\\+-]?\\d+([,\\.]\\d+)?([eE]-?\\d+)?$");
		System.out.println(re.matcher(num).matches());
		System.out.println(NumberUtils.isNumber(num));
	}
}
