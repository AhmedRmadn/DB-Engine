
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;
import java.util.concurrent.TimeUnit;

public class DBApp implements DBAppInterface {
	private Hashtable<String, Table> tables;
	private int numOfTables = 0;
	private static String metaPath = "src/main/resources/metadata.csv";

	// constructor
	public DBApp() {
		init();
	}

	@Override
	public void init() {
		try {
			Read();
		} catch (Exception e) {
			tables = new Hashtable<String, Table>();

			write();
		}

	}

	@Override
	public void createTable(String tableName, String clusteringKey, Hashtable<String, String> colNameType,
			Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) throws DBAppException {

		if (validCreate(tableName, clusteringKey, colNameType, colNameMin, colNameMax)) {
			numOfTables++;
			Table t = new Table(tableName, clusteringKey, colNameType.get(clusteringKey));
			tables.put(tableName, t);
			WriteMeta(tableName, clusteringKey, colNameType, colNameMin, colNameMax);

		} else {
			System.out.println("Invalid Table");
			throw new DBAppException();

		}
		write();

	}

	@Override
	public void createIndex(String tableName, String[] columnNames) throws DBAppException {
		/// System.out.println(this.tables.get(tableName).getPages());
		HashSet<String> col = new HashSet<String>();
		for (String s : columnNames)
			col.add(s);
		if (!validCreateIndex(tableName, col) || col.size() != columnNames.length) {
			throw new DBAppException();
		}
		editInedexed(tableName, col);

		this.tables.get(tableName).createIndex(columnNames);
		write();

	}

	private boolean validCreateIndex(String tableName, HashSet<String> col) {
		Vector<Object> data = loadMeta(tableName);
		Hashtable<String, String> type = (Hashtable<String, String>) data.get(1);
		for (String s : col) {
			if (!type.containsKey(s))
				return false;
		}
		return true;
	}

	@Override
	public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {

		if (valid(tableName, colNameValue)) {
			tables.get(tableName).insertIntoTable(colNameValue);
			write();
		} else {
			throw new DBAppException();
		}
	}

	@Override
	public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue)
			throws DBAppException {
		if (valid(tableName, columnNameValue)) {
			try {
				tables.get(tableName).updateTable(clusteringKeyValue, columnNameValue);
			} catch (Exception e) {
				e.printStackTrace();
			}

			write();
		} else {
			throw new DBAppException();
		}

	}

	@Override
	public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException {

		if (valid(tableName, columnNameValue)) {
			try {
				tables.get(tableName).deleteFromTable(columnNameValue);

			} catch (Exception e) {
				e.printStackTrace();
			}
			write();
		} else {
			throw new DBAppException();
		}

	}

	private boolean indexTerm(SQLTerm term, Hashtable<String, Boolean> index) {

		return index.get(term._strColumnName);
	}

	@Override
	public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {

		if (sqlTerms.length == 0 || !validSelect(sqlTerms, arrayOperators, sqlTerms[0]._strTableName))
			throw new DBAppException();

		Vector<HashSet<Hashtable<String, Object>>> finalSet = new Vector<HashSet<Hashtable<String, Object>>>();
		Vector<Object> data = loadMeta(sqlTerms[0]._strTableName);

		Table t = this.tables.get(sqlTerms[0]._strTableName);
		if (sqlTerms.length == 1) {
			SQLTerm[] termsForSelect = { sqlTerms[0] };
			HashSet<Hashtable<String, Object>> resSet = null;
			if (indexTerm(sqlTerms[0], (Hashtable<String, Boolean>) data.get(4))) {
				// System.out.println(termsForSelect[0]);
				resSet=t.selectWithIndex(termsForSelect);

			} else
				resSet= t.selectWithoutIndex(termsForSelect);
			
			//System.out.println(resSet);
			return resSet.iterator();
		}
		boolean andFlag = arrayOperators[0].equals("AND");
		HashSet<SQLTerm> te = new HashSet<SQLTerm>();
		boolean indexed = false;
		if (andFlag) {
			te.add(sqlTerms[0]);
			indexed = indexed || indexTerm(sqlTerms[0], (Hashtable<String, Boolean>) data.get(4));
		} else {
			SQLTerm[] termsForSelect = { sqlTerms[0] };
			if (indexTerm(sqlTerms[0], (Hashtable<String, Boolean>) data.get(4))) {
				// System.out.println(termsForSelect[0]);
				finalSet.add(t.selectWithIndex(termsForSelect));
			} else
				finalSet.add(t.selectWithoutIndex(termsForSelect));
		}

		for (int i = 1; i < sqlTerms.length; i++) {
			// System.out.println("ping");
			if (i == sqlTerms.length - 1 || arrayOperators[i].equals("OR") || arrayOperators[i].equals("XOR")) {
				if (andFlag) {
					te.add(sqlTerms[i]);
					indexed = indexed || indexTerm(sqlTerms[i], (Hashtable<String, Boolean>) data.get(4));
					SQLTerm[] arr = toArr(te);
					if (indexed)
						finalSet.add(t.selectWithIndex(arr));
					else
						finalSet.add(t.selectWithoutIndex(arr));
					andFlag = false;
					indexed = false;
					te = new HashSet<SQLTerm>();
				} else {
					SQLTerm[] termsForSelect = { sqlTerms[i] };
					if (indexTerm(sqlTerms[i], (Hashtable<String, Boolean>) data.get(4))) {
						// System.out.println("pong");
						finalSet.add(t.selectWithIndex(termsForSelect));
					} else
						finalSet.add(t.selectWithoutIndex(termsForSelect));
				}
			} else {
				te.add(sqlTerms[i]);
				andFlag = true;
				indexed = indexed || indexTerm(sqlTerms[i], (Hashtable<String, Boolean>) data.get(4));

			}
		}
		// System.out.println(finalSet);

		Vector<HashSet<Hashtable<String, Object>>> finalSet2 = new Vector<HashSet<Hashtable<String, Object>>>();
		HashSet<Hashtable<String, Object>> tempSet = finalSet.get(0);
		int j = 1;
		for (int i = 0; i < arrayOperators.length; i++) {
			if (arrayOperators[i].equals("OR")) {
				or(tempSet, finalSet.get(j++));
			} else if (arrayOperators[i].equals("XOR")) {
				finalSet2.add(tempSet);
				tempSet = finalSet.get(j++);
			}
		}
		finalSet2.add(tempSet);
		HashSet<Hashtable<String, Object>> resSet = finalSet2.get(0);
		finalSet2.remove(0);
		for (HashSet<Hashtable<String, Object>> s : finalSet2) {
			xor(resSet, s);
		}
		//System.out.println(resSet);
		return resSet.iterator();
	}

	private boolean validSelect(SQLTerm[] term, String[] op, String tableName) {
		if (term.length != op.length + 1)
			return false;

		for (String s : op) {
			if (s == null)
				return false;
			if (!(s.equals("OR") || s.equals("AND") || s.equals("XOR")))
				return false;
		}

		Hashtable<String, String> type = (Hashtable<String, String>) loadMeta(tableName).get(1);
		for (SQLTerm t : term) {
		//	System.out.println("ping");
			if (t == null || t._strTableName == null || t._strColumnName == null || t._objValue == null)
				return false;
			if (!(t._strTableName.equals(tableName) && type.containsKey(t._strColumnName) && t.validTerm()
					&& validObj(type.get(t._strColumnName), t._objValue))) {
				
				return false;
			}

		}
		
		return true;
	}

	private boolean validObj(String type, Object o) {
		// System.out.println();
		if (type.equals("java.lang.Integer"))
			return o instanceof Integer;
		else if (type.equals("java.lang.String"))
			return o instanceof String;
		else if (type.equals("java.lang.Double"))
			return o instanceof Double;
		else if (type.equals("java.util.Date"))
			return o instanceof Date;
		return false;
	}

	private static SQLTerm[] toArr(HashSet<SQLTerm> te) {
		SQLTerm[] arr = new SQLTerm[te.size()];
		int i = 0;
		for (SQLTerm t : te)
			arr[i++] = t;
		return arr;
	}

	private void and(HashSet<Hashtable<String, Object>> set1, HashSet<Hashtable<String, Object>> set2) {

		for (Hashtable<String, Object> ht : set1) {
			if (!set2.contains(ht))
				set1.remove(ht);

		}

	}

	private void or(HashSet<Hashtable<String, Object>> set1, HashSet<Hashtable<String, Object>> set2) {
		// HashSet<Hashtable<String, Object>> resSet = new HashSet<Hashtable<String,
		// Object>>();
		for (Hashtable<String, Object> ht : set2) {
			set1.add(ht);
		}

	}

	private void xor(HashSet<Hashtable<String, Object>> set1, HashSet<Hashtable<String, Object>> set2) {
		for (Hashtable<String, Object> ht : set1) {
			if (set2.contains(ht)) {
				set1.remove(ht);
				set2.remove(ht);
			}

		}
		for(Hashtable<String, Object> ht : set2) {
			set1.add(ht);
		}

	}

	private void Read() throws IOException, ClassNotFoundException {
		FileInputStream fi = new FileInputStream("src/main/resources/data/tables.ser");
		ObjectInputStream oi = new ObjectInputStream(fi);
		tables = (Hashtable<String, Table>) (oi.readObject());

		oi.close();

	}

	public void write() {
		FileOutputStream fs;
		try {
			fs = new FileOutputStream("src/main/resources/data/tables.ser");
			ObjectOutputStream os = new ObjectOutputStream(fs);
			os.writeObject(tables);
			os.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	private boolean validCreate(String tableName, String clusteringKey, Hashtable<String, String> colNameType,
			Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) {
		boolean[] v = { true };
		v[0] = v[0] && (!this.tables.containsKey(tableName));
		colNameType.forEach(
				(key, value) -> v[0] = v[0] && (colNameMin.containsKey(key) && validParse(value, colNameMin.get(key)))
						&& (colNameMax.containsKey(key) && validParse(value, colNameMax.get(key))));
		v[0] = v[0] && colNameType.containsKey(clusteringKey);
		return v[0];

	}

	private boolean validParse(String type, String value) {
		try {
			Object o = null;
			if (type.equals("java.lang.Integer"))
				o = Integer.parseInt(value);
			else if (type.equals("java.lang.String"))
				o = value;
			else if (type.equals("java.lang.Double"))
				o = Double.parseDouble(value);
			else if (type.equals("java.util.Date"))
				o = new SimpleDateFormat("yyyy-MM-dd").parse(value);

			return true;
		} catch (Exception e) {
			return false;
		}

	}

	private boolean valid(String tableName, Hashtable<String, Object> colNameValue) {
		boolean f = false;
		if (tables.containsKey(tableName)) {

			Table t = tables.get(tableName);
			Vector<Object> vMeta = loadMeta(tableName);
			Hashtable<String, String> typeHash = (Hashtable<String, String>) vMeta.get(1);
			Hashtable<String, String> maxHash = (Hashtable<String, String>) vMeta.get(3);
			Hashtable<String, String> minHash = (Hashtable<String, String>) vMeta.get(2);
			boolean[] v = new boolean[1];
			v[0] = true;
			colNameValue
					.forEach((key, value) -> v[0] = (val(tableName, key, value, typeHash, maxHash, minHash) && v[0]));
			f |= v[0];

		}
		return f;
	}

	private boolean val(String tableName, String key, Object value, Hashtable<String, String> typeHash,
			Hashtable<String, String> maxHash, Hashtable<String, String> minHash) {
		boolean f = false;
		if (typeHash.containsKey(key)) {
			String type = typeHash.get(key);
			String max = maxHash.get(key);
			String min = minHash.get(key);
			if (type.equals("java.lang.Integer")) {

				if (value instanceof Integer) {

					Integer n = (Integer) value;
					Integer ma = Integer.parseInt(max);
					Integer mi = Integer.parseInt(min);
					if (n.compareTo(ma) <= 0 && n.compareTo(mi) >= 0)
						f = true;
				}

			} else if (type.equals("java.lang.String")) {
				if (value instanceof String) {
					String n = (String) value;
					if (n.compareTo(max) <= 0 && n.compareTo(min) >= 0)
						f = true;
				}
			} else if (type.equals("java.lang.Double")) {
				if (value instanceof Double) {
					Double n = (Double) value;
					Double ma = Double.parseDouble(max);
					Double mi = Double.parseDouble(min);
					if (n.compareTo(ma) <= 0 && n.compareTo(mi) >= 0)
						f = true;
				}
			} else if (type.equals("java.util.Date")) {
				if (value instanceof Date) {
					Date n = (Date) value;
					Date ma = null;
					Date mi = null;
					try {
						ma = new SimpleDateFormat("yyyy-MM-dd").parse(max);
						mi = new SimpleDateFormat("yyyy-MM-dd").parse(min);
					} catch (ParseException e) {
						System.out.println("invalid");
						e.printStackTrace();
					}
					if (n.compareTo(ma) <= 0 && n.compareTo(mi) >= 0)
						f = true;
				}
			}
		}
		return f;

	}

	private void WriteMeta(String tableName, String clusteringKey, Hashtable<String, String> colNameType,
			Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) {
		colNameType.forEach((key, value) -> storeMeta(tableName, key, value, key == clusteringKey, false,
				colNameMin.get(key), colNameMax.get(key)));

	}

	private void storeMeta(String tableName, String columnName, String columnType, boolean clusteringKey,
			boolean indexed, String min, String max) {
		try {
			FileWriter fw = new FileWriter(metaPath, true);
			BufferedWriter bw = new BufferedWriter(fw);
			PrintWriter pw = new PrintWriter(bw);
			pw.println(tableName + "," + columnName + "," + columnType + "," + clusteringKey + "," + indexed + "," + min
					+ "," + max);
			pw.flush();
			pw.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void editInedexed(String tableName, HashSet<String> colNames) {
		// System.out.println("ping");
		try {
			String oldPath = this.metaPath;
			this.metaPath = "src/main/resources/metadataTemp.csv";
			BufferedReader br = new BufferedReader(new FileReader(oldPath));
			String record;
			while ((record = br.readLine()) != null) {
				String[] fields = record.split(",");
				if (fields[0].equals(tableName)) {
					if (colNames.contains(fields[1]))
						fields[4] = "True";

				}
				storeMeta(fields[0], fields[1], fields[2], Boolean.parseBoolean(fields[3]),
						Boolean.parseBoolean(fields[4]), fields[5], fields[6]);
			}
			br.close();

			File newFile = new File(this.metaPath);
			File oldFile = new File(oldPath);
			// System.out.println(Runtime.getRuntime().exec(oldPath));
			oldFile.delete();
			// TimeUnit.MINUTES.sleep(2);
			this.metaPath = oldPath;
			File te = new File(oldPath);
			newFile.renameTo(te);
			// System.out.println("pong");
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static Vector<Object> loadMeta(String tableName) {
		try {
			BufferedReader br = new BufferedReader(new FileReader(metaPath));
			Hashtable<String, String> type = new Hashtable<String, String>();
			Hashtable<String, String> max = new Hashtable<String, String>();
			Hashtable<String, String> min = new Hashtable<String, String>();
			Hashtable<String, Boolean> index = new Hashtable<String, Boolean>();
			String cluKey = null;
			String record;
			while ((record = br.readLine()) != null) {
				String[] fields = record.split(",");
				if (fields[0].equals(tableName)) {
					type.put(fields[1], fields[2]);
					min.put(fields[1], fields[5]);
					max.put(fields[1], fields[6]);
					index.put(fields[1], Boolean.parseBoolean(fields[4]));
					if (fields[3].equals("true")) {
						cluKey = fields[1];
					}
				}
			}
			Vector<Object> v = new Vector<Object>();
			v.add(cluKey);
			v.add(type);
			v.add(min);
			v.add(max);
			v.add(index);
			br.close();
			return v;

		} catch (Exception e) {
			System.out.println("error in meta file");
			e.printStackTrace();
			return null;
		}

	}

	public void printTable(String name) {
		this.tables.get(name).print();
	}

	public Table getTable(String s) {
		return this.tables.get(s);
	}

}
