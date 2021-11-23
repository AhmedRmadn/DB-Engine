

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Date;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Vector;

public class Page implements Serializable {
	private int numOfRecords;
	private int maxNum;
	private String path;
	private Object minV;
	private Object maxV;
	private String primryKey;

	public Page(int maxNum, String path, String primryKey) {
		this.maxNum = maxNum;
		this.numOfRecords = 0;
		this.path = path;
		this.primryKey = primryKey;
	}

	private Vector<Hashtable<String, Object>> Read() throws IOException, ClassNotFoundException {
		Vector<Hashtable<String, Object>> Records;
		FileInputStream fi = new FileInputStream(this.path);
		ObjectInputStream oi = new ObjectInputStream(fi);
		Records = (Vector<Hashtable<String, Object>>) (oi.readObject());
		oi.close();
		return Records;
	}
	
	public Vector<Hashtable<String, Object>> getRecords(){
		try {
			Vector<Hashtable<String, Object>> Records = Read();
			return Records;
		} catch (ClassNotFoundException | IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	public void write(Vector<Hashtable<String, Object>> Records) {
		FileOutputStream fs;
		try {
			fs = new FileOutputStream(this.path);
			ObjectOutputStream os = new ObjectOutputStream(fs);
			os.writeObject(Records);
			os.close();

		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	
	public void searchForSelect(HashSet<Hashtable<String, Object>> set , SQLTerm term) {
		try {
		Vector<Hashtable<String, Object>> Records = Read();
		for(int i = 0 ; i < Records.size();i++) {
			Hashtable<String, Object> tuple = Records.get(i);
			if(term.sat(tuple.get(term._strColumnName)))
				set.add(tuple);
		}
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void insert(Hashtable<String, Object> tuple) {
		Vector<Hashtable<String, Object>> Records;

		try {
			Records = Read();
			InsertSortedBinary(Records, tuple);
			write(Records);
			this.minV = Records.get(0).get(primryKey);
			this.maxV = Records.get(Records.size() - 1).get(primryKey);
			this.numOfRecords++;

		} catch (FileNotFoundException e) {
			Records = new Vector<Hashtable<String, Object>>();
			InsertSortedBinary(Records, tuple);
			write(Records);
			this.minV = Records.get(0).get(primryKey);
			this.maxV = Records.get(Records.size() - 1).get(primryKey);
			this.numOfRecords++;

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public void deleteFromTable(Hashtable<String, Object> htblColNameValue,HashSet<Object> PKs) {
		if (htblColNameValue.containsKey(primryKey))
			deleteWithClu(htblColNameValue,PKs);
		else
			deleteWithoutClu(htblColNameValue,PKs);

	}

	private void deleteWithoutClu(Hashtable<String, Object> htblColNameValue,HashSet<Object> PKs) {
		Vector<Hashtable<String, Object>> Records;
		try {
			Records = Read();
			for (int i = 0; i < Records.size(); i++) {
				if (validForDel(Records.get(i), htblColNameValue)) {
					PKs.add(Records.get(i).get(primryKey));
					Records.remove(i);
					i--;
				}
			}
			this.numOfRecords = Records.size();
			if (this.numOfRecords <= 0) {
				this.minV = null;
				this.maxV = null;
			} else {
				this.minV = Records.get(0).get(primryKey);
				this.maxV = Records.get(Records.size() - 1).get(primryKey);
			}

			write(Records);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	

	private void deleteWithClu(Hashtable<String, Object> htblColNameValue,HashSet<Object> PKs) {
		Vector<Hashtable<String, Object>> Records;
		try {
			Records = Read();
			int index = binarySearch(Records, htblColNameValue.get(primryKey));
			if (index >= 0 && index < Records.size() && validForDel(Records.get(index), htblColNameValue)) {
				PKs.add(Records.get(index).get(primryKey));
				Records.remove(index);
			}
			this.numOfRecords = Records.size();
			if (this.numOfRecords <= 0) {
				this.minV = null;
				this.maxV = null;
			} else {
				this.minV = Records.get(0).get(primryKey);
				this.maxV = Records.get(Records.size() - 1).get(primryKey);
			}
			write(Records);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private boolean validForDel(Hashtable<String, Object> tuple, Hashtable<String, Object> htblColNameValue) {
		boolean[] v = new boolean[1];
		v[0] = true;

		htblColNameValue.forEach((key,
				value) -> v[0] = tuple.containsKey(key) && tuple.get(key).equals(htblColNameValue.get(key)) && v[0]);
		return v[0];
	}

	public Hashtable<String, Object> updateTable(Object primaryValue, Hashtable<String, Object> htblColNameValue) {
		Vector<Hashtable<String, Object>> Records;
		try {
			Records = Read();
			int ind = this.binarySearch(Records, primaryValue);
			if (ind >= 0 && ind < Records.size()) {
				Hashtable<String, Object> tuble = Records.get(ind);
				htblColNameValue.forEach((key, value) -> tuble.put(key, value));
				write(Records);
				return tuble;
				// Records.set(ind, tuble);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}


	public Vector<Hashtable<String, Object>> load() {
		Vector<Hashtable<String, Object>> Records;
		Vector<Hashtable<String, Object>> ans = new Vector<Hashtable<String, Object>>();

		try {
			Records = Read();
			int size = (int) ((Records.size() + 1) / 2);
			for (int i = size; i < Records.size();) {
				ans.add(Records.get(i));
				Records.remove(i);
			}
			write(Records);
			this.minV = Records.get(0).get(primryKey);
			this.maxV = Records.get(Records.size() - 1).get(primryKey);
			this.numOfRecords = Records.size();

		} catch (Exception e) {
			e.printStackTrace();
		}
		return ans;
	}

	public void store(Vector<Hashtable<String, Object>> x) {
		Vector<Hashtable<String, Object>> Records;

		try {
			Records = Read();
			Records = x;
			write(Records);
			this.minV = Records.get(0).get(primryKey);
			this.maxV = Records.get(Records.size() - 1).get(primryKey);
			this.numOfRecords = x.size();

		} catch (FileNotFoundException e) {
			Records = x;
			write(Records);
			this.minV = Records.get(0).get(primryKey);
			this.maxV = Records.get(Records.size() - 1).get(primryKey);
			this.numOfRecords = x.size();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void InsertSortedBinary(Vector<Hashtable<String, Object>> Records, Hashtable<String, Object> tuple) {
		int index = binarySearch(Records, tuple.get(primryKey));
		if (index < 0)
			index += (2 * Records.size());
		Records.add(index, tuple);
	}

	private int binarySearch(Vector<Hashtable<String, Object>> Records, Object primryValue) {
		int l = 0, r = Records.size() - 1;
		while (l <= r) {
			int m = l + (r - l) / 2;
			if (((Comparable) (Records.get(m).get(primryKey))).compareTo(primryValue) == 0)
				return m;
			if (((Comparable) (Records.get(m).get(primryKey))).compareTo(primryValue) < 0)
				l = m + 1;
			else
				r = m - 1;
		}
		return l - (2 * Records.size());
	}
	
	public Hashtable<String, Object> getTupleBin(Object primryValue){
		try {
		Vector<Hashtable<String, Object>> Records = Read();
		int l = 0, r = Records.size() - 1;
		while (l <= r) {
			int m = l + (r - l) / 2;
			if (((Comparable) (Records.get(m).get(primryKey))).compareTo(primryValue) == 0)
				return Records.get(m);
			if (((Comparable) (Records.get(m).get(primryKey))).compareTo(primryValue) < 0)
				l = m + 1;
			else
				r = m - 1;
		}
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		return null;
		
	}

	/// SETTER AND GETTER

	public Object getMinV() {
		return minV;
	}

	public void setMinV(Object minV) {
		this.minV = minV;
	}

	public Object getMaxV() {
		return maxV;
	}

	public String getPath() {
		return path;
	}

	public boolean isFull() {
		return this.maxNum == this.numOfRecords;
	}

	public boolean isEmpty() {
		return this.numOfRecords == 0;
	}

	///// toString for testing
//	@Override
//	public String toString() {
//		Vector<Hashtable<String, Object>> Records = new Vector<Hashtable<String, Object>>();
//		try {
//			Records = Read();
//		} catch (ClassNotFoundException | IOException e) {
//			e.printStackTrace();
//		}
//
//		boolean f = true;
//		for (int i = 1; i < Records.size() && f; i++) {
//			if (((Comparable) (Records.get(i).get(primryKey))).compareTo(Records.get(i - 1).get(primryKey)) < 0)
//				f = false;
//		}
//
//		return Records.toString() + " numodRec " + this.minV + "  " + this.maxV + "  " + f + " " + this.numOfRecords;
//	}

}
