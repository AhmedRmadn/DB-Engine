import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

public class Bucket implements Serializable {

	// private Hashtable<Vector<Object>,Hashtable<Object, Page>> ref ;
	private String path;
	private int numOfRecords;
	private int maxNum = 100;
	private static int numOfB = getNumofRow();

	public Bucket() {
		this.path = "src/main/resources/data/" + numOfB + ".ser";
		write(new Hashtable<Vector<Object>, Hashtable<Object, Page>>());
		numOfB++;
		this.numOfRecords = 0;
	}

	private Hashtable<Vector<Object>, Hashtable<Object, Page>> Read() throws IOException, ClassNotFoundException {
		Hashtable<Vector<Object>, Hashtable<Object, Page>> Records;
		FileInputStream fi = new FileInputStream(this.path);
		ObjectInputStream oi = new ObjectInputStream(fi);
		Records = (Hashtable<Vector<Object>, Hashtable<Object, Page>>) (oi.readObject());
		oi.close();
		return Records;
	}

	private void write(Hashtable<Vector<Object>, Hashtable<Object, Page>> Records) {
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

	public void add(Vector<Object> record, Object primaryValue, Page p) {
		if (path.equals("src/main/resources/data/" + 41 + ".ser"))
			System.out.println("founded");
		try {
			Hashtable<Vector<Object>, Hashtable<Object, Page>> v = Read();
			if (v.containsKey(record)) {
				if (!v.containsKey(primaryValue))
					this.numOfRecords++;
				v.get(record).put(primaryValue, p);

			} else {
				v.put(record, new Hashtable<Object, Page>());
				v.get(record).put(primaryValue, p);
				this.numOfRecords += 1;
			}
			write(v);
		} catch (Exception e) {
			Hashtable<Vector<Object>, Hashtable<Object, Page>> v = new Hashtable<Vector<Object>, Hashtable<Object, Page>>();
			v.put(record, new Hashtable<Object, Page>());
			v.get(record).put(primaryValue, p);
			this.numOfRecords = 1;
			write(v);
		}
	}

	public void deleteRows(HashSet<Object> PKs) {
		try {
			Hashtable<Vector<Object>, Hashtable<Object, Page>> v = Read();
			Hashtable<Vector<Object>, Hashtable<Object, Page>> temp = (Hashtable<Vector<Object>, Hashtable<Object, Page>>) v.clone();
			Set<Vector<Object>> s = temp.keySet();
			for (Vector<Object> key : s) {
				Hashtable<Object, Page> tempRe = (Hashtable<Object, Page>) v.get(key).clone();
				Set<Object> allkeys = tempRe.keySet();
				for (Object primary : allkeys) {
					if (PKs.contains(primary)) {
						v.get(key).remove(primary);
						this.numOfRecords--;
						if (v.get(key).isEmpty()) {
							v.remove(key);
						}
					}
				}
			}
			write(v);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void searchSelect(String[] columnNames, HashSet<Hashtable<String, Object>> set,
			Hashtable<String, SQLTerm> terms) {
		  //System.out.println(terms);
		
		try {
			Hashtable<Vector<Object>, Hashtable<Object, Page>> v = Read();
			//System.out.println(v);
			Set<Vector<Object>> s = v.keySet();
			for (Vector<Object> key : s) {
				validForInsert(columnNames, key, v.get(key), set, terms);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void validForInsert(String[] columnNames, Vector<Object> key, Hashtable<Object, Page> value,
			HashSet<Hashtable<String, Object>> set, Hashtable<String, SQLTerm> terms) {
		boolean valid = true;
		// System.out.println(terms +" "+Arrays.toString(columnNames));
		for (int i = 0; i < key.size(); i++) {
			if (terms.containsKey(columnNames[i])) {

				valid = valid & terms.get(columnNames[i]).sat(key.get(i));
				//System.out.println(terms.get(columnNames[i]) + " " + key.get(i) + " " + terms.get(columnNames[i]).sat(key.get(i)));

			}
		}
		if (valid) {
			Set<Object> s = value.keySet();
			for (Object prim : s) {
				set.add(value.get(prim).getTupleBin(prim));
			}
		}

	}

	public int getNumOfRecords() {
		return numOfRecords;
	}

	public boolean hasSpace() {
		return this.numOfRecords < this.maxNum;
	}

	public boolean isEmpty() {
		return this.numOfRecords == 0;
	}

	public String getPath() {
		return path;
	}
	
	private static int getNumofRow() {
		try (FileReader reader = new FileReader("src/main/resources/DBApp.config")) {
			Properties properties = new Properties();
			properties.load(reader);
			String s = properties.getProperty("MaximumKeysCountinIndexBucket");
			return Integer.parseInt(s);

		} catch (Exception e) {
			;
			e.printStackTrace();
		}
		return -1;

	}

}
