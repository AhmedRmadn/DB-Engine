import java.io.File;
import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Vector;
import java.util.concurrent.TimeUnit;



public class Grid implements Serializable{
	private String[] columnNames;
	private Vector<Vector<Range>> GridRanges;
	private Vector<Vector<Bucket>> GridValues;
	private long size;
	private int MaxNumberInBucket = 100;

	public Grid(String[] columnNames, Table t) {
		this.columnNames = columnNames;
		this.GridRanges = new Vector<Vector<Range>>();
		this.GridValues = new Vector<Vector<Bucket>>();
		size = (long) Math.pow(10, columnNames.length);
		
		createRange(t);
		init(t);
		createIndex(t);
	}

	private void init(Table t) {
		
		for (int i = 0; i < size; i++) {
			Vector<Bucket> v = new Vector<Bucket>();
			v.add(new Bucket());
			this.GridValues.add(v);
		}

	}

	public void createIndex(Table t) {
		//
		//System.out.println(t.getPages());
		Vector<Page> pages = t.getPages();
		for (int i = 0; i < pages.size(); i++) {
			
			Vector<Hashtable<String, Object>> curPage = pages.get(i).getRecords();
			for (int j = 0; j < curPage.size(); j++) {
				
                  addRow(pages.get(i),curPage.get(j),t);
			}
		}

	}
	public void addRow(Page p , Hashtable<String, Object> row ,Table t ) {
		Vector<Object> recordIndex = new Vector<Object>();
		int index = extractValues(row, recordIndex);
         //System.out.println(this.GridValues.size());
		Vector<Bucket> cell = this.GridValues.get(index);
		
		for(int i = 0 ; i <cell.size();i++) {
			if(cell.get(i).hasSpace()) {
				cell.get(i).add(recordIndex, row.get(t.getClusteringKey()), p);
				return;
			}
		}
		Bucket b = new Bucket();
		cell.add(b);
		b.add(recordIndex, row.get(t.getClusteringKey()), p);
		
	}
	
	public void deleteRows(HashSet<Object> PKs) {
		for(Vector<Bucket> vb : this.GridValues) {
			for(Bucket b : vb) {
				b.deleteRows(PKs);
				if(b.isEmpty()) {
					if(vb.size()>1) {
						vb.remove(b);
						delectBucketFile(b);
					}
				}
				
			}
		}
	}
	
	private void delectBucketFile(Bucket b) {
		File myFile = new File(b.getPath());
		myFile.delete();
	}
    //// take record and extract its index in grid
	private int extractValues(Hashtable<String, Object> record, Vector<Object> v) {
		String x = "";
		for (int i = 0; i < this.columnNames.length; i++) {
			Object o = record.get(this.columnNames[i]);
			v.add(o);
			int temp = getRangesBin(i, o);
		//	System.out.println(temp +" "+o+" "+GridRanges.get(i) );
			x = x + temp;
			//System.out.println(GridRanges.get(i).get(9).pos(o) +" "+ o);
			
		}
		
		return Integer.parseInt(x);
	}

	private void createRange(Table t) {
        Vector<Object> data =  DBApp.loadMeta(t.getTableName());
		Hashtable<String, String> columnTypes = (Hashtable<String, String>)data.get(1);
		Hashtable<String, String> min = (Hashtable<String, String>) data.get(2);
		Hashtable<String, String> max = (Hashtable<String, String>) data.get(3);

		for (int i = 0; i < this.columnNames.length; i++) {

			Vector<Range> v = null;
			String type = columnTypes.get(columnNames[i]);
			String minValue = min.get(columnNames[i]);
			String maxValue = max.get(columnNames[i]);
			try {
				if (type.equals("java.lang.Integer"))
					v = this.createIntegerRange(Integer.parseInt(minValue), Integer.parseInt(maxValue));
				else if (type.equals("java.lang.String"))
					v = this.createStringRange(minValue, maxValue);
				else if (type.equals("java.lang.Double"))
					v = this.createDoubleRange(Double.parseDouble(minValue), Double.parseDouble(maxValue));
				else if (type.equals("java.util.Date")) {
					v = this.createDateRange(new SimpleDateFormat("yyyy-MM-dd").parse(minValue),
							new SimpleDateFormat("yyyy-MM-dd").parse(maxValue));
				}
			} catch (Exception e) {
				e.printStackTrace();
			}

			GridRanges.add(v);
		}

	}

	
	//////type of Ranges
	private Vector<Range> createIntegerRange(int start, int end) {
		int step = (end - start + 1) / 10;
		int r = (end - start + 1) % 10;
		Vector<Range> v = new Vector<Range>();
		int c = start;
		v.add(new Range(c, c + (r / 2) + step));
		c = c + (r / 2) + step;
		for (int i = 2; i < 10; i++) {
			v.add(new Range(c, c + step));
			c += step;
		}
		v.add(new Range(c, c + step + ((r + 1) / 2)));
		return v;
	}

	private Vector<Range> createDoubleRange(double start, double end) {
		double step = (end - start) / 10;

		Vector<Range> v = new Vector<Range>();
		double c = start;
		for (int i = 1; i < 10; i++) {
			v.add(new Range(c, c + step));
			c += step;
		}
		v.add(new Range(c, c + step+1));
		return v;
	}

	private Vector<Range> createStringRange(String start, String end) {

		int step = 26 / 10;
		int r = (26) % 10;
		Vector<Range> v = new Vector<Range>();
		char x  = 'A';
		if(start.charAt(0)>='0'&&start.charAt(0)<='9') {
			step=1;
			r=0;
			x='0';
		}
		//String c = x+"";
		v.add(new Range(x+"", ((char)(x + step + (r / 2)))+""));
		x = (char) (x + (r / 2) + step);
		
		for (int i = 2; i < 10; i++) {
			v.add(new Range(x+"", ((char)(x + step))+""));
			x =(char) (x+step);
		}
		v.add(new Range(x+"", ((char)(x + step + ((r + 1) / 2)))+""));
		return v;
	}



	private static int getDifferenceDays(Date d1, Date d2) {
		long diff = (long) (d2.getTime() - d1.getTime());
		return (int) TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS);
	}

	private static Date addDays(Date d, int numDays) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(d);
		calendar.add(Calendar.DAY_OF_MONTH, numDays);
		return calendar.getTime();
	}

	private Vector<Range> createDateRange(Date start, Date end) {
		//System.out.println(start+"  "+end);
		int step = (getDifferenceDays(start, end) + 1) / 10;
		//System.out.println(step + " data");
		int r = (getDifferenceDays(start, end) + 1) % 10;
		Vector<Range> v = new Vector<Range>();
		Date c = start;
		v.add(new Range(c, addDays(c, (r / 2) + step)));
		c = addDays(c, (r / 2) + step);
		for (int i = 2; i < 10; i++) {
			v.add(new Range(c, addDays(c, step)));
			c = addDays(c, step);
		}
		v.add(new Range(c, addDays(c, step + ((r + 1) / 2))));
		return v;
	}
	
	///////////end Ranges
    ////Binary Search in Ranges
	private int getRangesBin(int rangeIndex, Object o) {
		Vector<Range> range = this.GridRanges.get(rangeIndex);
		int l = 0, r = range.size() - 1;
		while (l <= r) {
			int m = l + (r - l) / 2;
			//System.out.print(m+" ");
			int x = range.get(m).pos(o);
			if (x > 0)
				l = m + 1;
			else if (x < 0)
				r = m - 1;
			else {
				//System.out.println();
				return m;
			}
		}
		//System.out.print(-1 + " "+o + GridRanges.get(rangeIndex));
	//	System.out.println();
		return -1;
	}
	
	////// 

	public void SearchForSelect(HashSet<Hashtable<String, Object>> set,Hashtable<String, SQLTerm> terms) {
		Vector<Vector<Bucket>> buckets = getBucketsOfValues(terms);
		for(int i = 0 ; i < buckets.size();i++) {
			for(int j = 0 ; j<buckets.get(i).size();j++) {
				buckets.get(i).get(j).searchSelect(columnNames, set, terms);
			}
		}

	}

	public Vector<Vector<Bucket>> getBucketsOfValues(Hashtable<String, SQLTerm> terms) {
		Integer[] colValues = new Integer[this.columnNames.length];
		for (int i = 0; i < colValues.length; i++) {
			if (terms.containsKey(this.columnNames[i])) {
				colValues[i] = getRangesBin(i, terms.get(this.columnNames[i])._objValue);
			}

		}
		Vector<Vector<Bucket>> res = new Vector<Vector<Bucket>>();
		getBucketsOfValues(terms, colValues, 0, 0, this.columnNames.length - 1, res);
		return res;

	}

	private void getBucketsOfValues(Hashtable<String, SQLTerm> terms, Integer[] colValues, int x, int i, int depth,
			Vector<Vector<Bucket>> res) {
		if (i == colValues.length) {
			res.add(this.GridValues.get(x));
		} else if (colValues[i] == null) {
			for (int j = 0; j < 10; j++) {
				getBucketsOfValues(terms, colValues, x + ((int) (Math.pow(10, depth) * j)), i + 1, depth - 1, res);
			}
		} else {
			SQLTerm t = terms.get(this.columnNames[i]);
			if (t._strOperator.equals("!="))
				for (int j = 0; j < 10 && j != colValues[i]; j++)
					if (j == colValues[i])
						continue;
					else
						getBucketsOfValues(terms, colValues, x + ((int) (Math.pow(10, depth) * j)), i + 1, depth - 1,
								res);

			else if (t._strOperator.equals("<"))
				for (int j = 0; j < colValues[i]; j++)
					getBucketsOfValues(terms, colValues, x + ((int) (Math.pow(10, depth) * j)), i + 1, depth - 1, res);

			else if (t._strOperator.equals("<="))
				for (int j = 0; j <= colValues[i]; j++)
					getBucketsOfValues(terms, colValues, x + ((int) (Math.pow(10, depth) * j)), i + 1, depth - 1, res);

			else if (t._strOperator.equals(">"))
				for (int j = colValues[i] + 1; j < 10; j++)
					getBucketsOfValues(terms, colValues, x + ((int) (Math.pow(10, depth) * j)), i + 1, depth - 1, res);

			else if (t._strOperator.equals(">="))
				for (int j = colValues[i]; j < 10; j++)
					getBucketsOfValues(terms, colValues, x + ((int) (Math.pow(10, depth) * j)), i + 1, depth - 1, res);

			else
				getBucketsOfValues(terms, colValues, x + ((int) (Math.pow(10, depth) * colValues[i])), i + 1, depth - 1,
						res);
		}

	}
	
	public HashSet<String> setOfCol(){
		HashSet<String> set = new HashSet<String>();
		for(String s :columnNames)
			set.add(s);
		return set;
	}

	@Override
	public String toString() {
		return "Grid [columnNames=" + Arrays.toString(columnNames) + "]";
	}

	public static void main(String[] args) {

		// Grid g = new Grid();
		// System.out.println(g.createDateRange(new Date(2011 - 1900, 4 - 1, 1), new
		// Date(2011 - 1900, 4 - 1, 11)));
	}

//	private boolean pageCon(Hashtable<String, Object> record, Hashtable<String, Range> ranges) {
//		boolean[] v = new boolean[1];
//		v[0] = true;
//
//		ranges.forEach((key, value) -> v[0] = record.containsKey(key) && value.hasValue(record.get(key)) && v[0]);
//		return v[0];
//	}
//
//	private Hashtable<String, Range> extractRanges(int index) {
//		Hashtable<String, Range> ranges = new Hashtable<String, Range>();
//		String s = String.valueOf(index);
//		String zeros = "";
//		for (int i = 0; i < s.length() - this.GridRanges.size(); i++) {
//			zeros = zeros + "0";
//		}
//		s = zeros + s;
//		for (int i = 0; i < s.length(); i++) {
//			int x = Integer.parseInt(s.charAt(i) + "");
//			ranges.put(this.columnNames[i], this.GridRanges.get(i).get(x));
//		}
//		return ranges;
//
//	}

}

class Range implements Comparable ,Serializable {
	Object start;
	Object end;

	public Range(Object start, Object end) {
		
		this.start = start;
		this.end = end;
	}

	@Override
	public int compareTo(Object arg0) {
		// TODO Auto-generated method stub
		return 0;
	}

	public boolean hasValue(Object o) {
		if(o instanceof String) {
			String s = String.valueOf(o);
			o=s.toUpperCase();
		}
		     return ((Comparable) (start)).compareTo(o) <= 0 && ((Comparable) (end)).compareTo(o) > 0;
	}

	public int pos(Object o) {
		if(o instanceof String) {
			String s = String.valueOf(o);
			o=s.toUpperCase();
		}
		int s = ((Comparable) (start)).compareTo(o);
		int e = ((Comparable) (end)).compareTo(o);
		if (s <= 0 && e > 0)
			return 0;
		else if (s <= 0)
			return 1;
		else
			return -1;
	}

	@Override
	public String toString() {
		return "Range [start=" + start + ", end=" + end + "]";
	}

	///////

}
