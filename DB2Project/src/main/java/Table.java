
import java.io.File;
import java.io.FileReader;
import java.io.Serializable;
import java.nio.file.Files;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Vector;

public class Table implements Serializable {
	private String tableName;
	private String clusteringKey;
	private String type;
	private Vector<Page> pages;
	private Vector<Grid> grids;
	private int NumOfPages;
	private int MaxNumOfRows;
	private int NumOfGrids;

	// constructor
	public Table(String tableName, String clusteringKey, String type) {
		this.tableName = tableName;
		this.clusteringKey = clusteringKey;
		this.pages = new Vector<Page>();
		this.grids = new Vector<Grid>();
		this.NumOfPages = -1;
		this.type = type;
		this.MaxNumOfRows = this.getNumofRow();
	}

	public void insertIntoTable(Hashtable<String, Object> colNameValue) {
		// Page page = null;
		Vector<Hashtable<String, Object>> records = new Vector<Hashtable<String, Object>>();
		records.add(colNameValue);
       
		if (NumOfPages == -1) {

			NumOfPages++;
			Page p = new Page(this.MaxNumOfRows, "src/main/resources/data/" + this.tableName + "" + NumOfPages + ".ser",
					this.clusteringKey);
			pages.add(p);
			p.insert(colNameValue);
			insertInGrid(p, records);
		} else {
			int index = this.getThePageBin(colNameValue.get(clusteringKey));

			if (!this.pages.get(index).isFull()) {
				this.pages.get(index).insert(colNameValue);
				insertInGrid(this.pages.get(index), records);

			} else {
				NumOfPages++;
				Page p = new Page(this.MaxNumOfRows,
						"src/main/resources/data/" + this.tableName + "" + NumOfPages + ".ser", this.clusteringKey);
				pages.add(index + 1, p);
				Vector<Hashtable<String, Object>> recordLoad = this.pages.get(index).load();
				p.store(recordLoad);
				index = this.getThePageBin(colNameValue.get(clusteringKey));
				this.pages.get(index).insert(colNameValue);
				insertInGrid(this.pages.get(index), records);
				insertInGrid(p, recordLoad);

			}
		}
		// System.out.println(pages);

	}

	private void insertInGrid(Page page, Vector<Hashtable<String, Object>> records) {
		for (Grid g : this.grids) {
			for (Hashtable<String, Object> ht : records)
				g.addRow(page, ht, this);
		}
	}

	public void updateTable(String clusteringKeyValue, Hashtable<String, Object> columnNameValue) {
		int index = getIndexRangeBin(parseType(clusteringKeyValue));
		if (index >= 0 && index < this.pages.size()) {
			Hashtable<String, Object> tuble = this.pages.get(index).updateTable(parseType(clusteringKeyValue),
					columnNameValue);
				this.deleteFromTable(tuble);
				this.insertIntoTable(tuble);


		}
	}

	public void deleteFromTable(Hashtable<String, Object> htblColNameValue) {
		HashSet<Object> PKs = new HashSet<Object>();
		if (htblColNameValue.containsKey(clusteringKey)) {
			int index = getIndexRangeBin(htblColNameValue.get(clusteringKey));
			if (index >= 0 && index < this.pages.size()) {
				this.pages.get(index).deleteFromTable(htblColNameValue,PKs);
				if (this.pages.get(index).getMaxV() == null || this.pages.get(index).getMinV() == null) {
					delectPageFile(this.pages.get(index));
					this.pages.remove(this.pages.get(index));
				}
			}
			//PKs.add(htblColNameValue.get(clusteringKey));
		} else {
			for (int i = 0; i < this.pages.size(); i++) {
				this.pages.get(i).deleteFromTable(htblColNameValue,PKs);
				if (this.pages.get(i).getMaxV() == null || this.pages.get(i).getMinV() == null) {
					delectPageFile(this.pages.get(i));
					this.pages.remove(i);
					i--;
				}
			}
		}
		deleteFromGrid(PKs);
	}
	
	public void deleteFromGrid(HashSet<Object> PKs) {
		for(Grid g : this.grids)
			g.deleteRows(PKs);
	}

	public HashSet<Hashtable<String, Object>> selectWithIndex(SQLTerm[] termOfSelect) {
		//System.out.println(grids);
		int gridIndex = getGrid(termOfSelect);
		Hashtable<String, SQLTerm> terms = new Hashtable<String, SQLTerm>();
		HashSet<SQLTerm> remTerms = new HashSet<SQLTerm>();
		//System.out.println(this.grids.size()+" "+gridIndex+" "+tableName);
		HashSet<String> gridSet = this.grids.get(gridIndex).setOfCol();
		for (int i = 0; i < termOfSelect.length; i++) {
			if (gridSet.contains(termOfSelect[i]._strColumnName))
				terms.put(termOfSelect[i]._strColumnName, termOfSelect[i]);
			else
				remTerms.add(termOfSelect[i]);
		}

		HashSet<Hashtable<String, Object>> set = new HashSet<Hashtable<String, Object>>();
		this.grids.get(gridIndex).SearchForSelect(set, terms);
		checkRem(set, remTerms);
		return set;

	}

	private void checkRem(HashSet<Hashtable<String, Object>> set, HashSet<SQLTerm> remTerms) {
		for (Hashtable<String, Object> ht : set) {
			for (SQLTerm term : remTerms) {
				if (!term.sat(ht.get(term._strColumnName))) {
					set.remove(ht);
					break;
				}
			}
		}
	}

	public int getGrid(SQLTerm[] term) {
		//System.out.println(grids);
		for (int x = (int) Math.pow(2, term.length)-1; x > 0; x--) {
			int n = getGrid(term, x);
			if (n >= 0)
				return n;
		}
		return -1;

	}

	public int getGrid(SQLTerm[] term, int x) {
		HashSet<String> set = new HashSet<String>();
		for (SQLTerm s : term) {
			if (x % 2 == 1)
				set.add(s._strColumnName);
			x = x >> 1;
		}
		//System.out.println(set);
		int m = 1000000000;
		int indexM = -1;
		boolean g = false;
		for (int i = 0; i < this.grids.size(); i++) {
			if (set.equals(grids.get(i).setOfCol()))
				return i;
			//System.out.println(set+"  ****  "+grids.get(i).setOfCol());
			if (set.containsAll(grids.get(i).setOfCol()) && !g) {
				//System.out.println("ss");
				int dif = set.size() - grids.get(i).setOfCol().size();
				if (dif < m) {
					m = dif;
					indexM = i;
				}
			}
			if (grids.get(i).setOfCol().containsAll(set)) {
				//System.out.println("oo");
				int dif = grids.get(i).setOfCol().size() - set.size();
				//System.out.println(dif + " "+m);
				if (dif < m) {
					
					m = dif;
					indexM = i;
					g = true;
				}
			}

		}
		return indexM;
	}

	public HashSet<Hashtable<String, Object>> selectWithoutIndex(SQLTerm[] term) {
		HashSet<Hashtable<String, Object>> set = new HashSet<Hashtable<String, Object>>();
		for (int i = 0; i < this.pages.size(); i++) {
			this.pages.get(i).searchForSelect(set, term[0]);
		}
		HashSet<SQLTerm> remTerms = new HashSet<SQLTerm>();
		for (int i = 1; i < term.length; i++)
			remTerms.add(term[i]);
		checkRem(set, remTerms);
		return set;
	}

	private int getIndexRangeBin(Object target) {
		int l = 0, r = this.pages.size() - 1;
		while (l <= r) {
			int m = l + (r - l) / 2;
			if ((((Comparable) target).compareTo((Comparable) (this.pages.get(m).getMaxV()))) <= 0
					&& (((Comparable) target).compareTo((Comparable) (this.pages.get(m).getMinV()))) >= 0) {
				return m;
			}
			if ((((Comparable) target).compareTo((Comparable) (this.pages.get(m).getMaxV()))) > 0) {
				l = m + 1;
				;
			} else
				r = m - 1;

		}
		return -1;
	}

	private int getThePageBin(Object target) {
		if (this.pages.size() > 0
				&& (((Comparable) target).compareTo((Comparable) (this.pages.get(0).getMinV()))) <= 0) {
			return 0;

		} else if (this.pages.size() > 0 && (((Comparable) target)
				.compareTo((Comparable) (this.pages.get(this.pages.size() - 1).getMaxV()))) >= 0) {
			return this.pages.size() - 1;
		}
		int l = 0, r = this.pages.size() - 1;
		while (l <= r) {
			int m = l + (r - l) / 2;
			if ((((Comparable) target).compareTo((Comparable) (this.pages.get(m).getMaxV()))) <= 0
					&& (((Comparable) target).compareTo((Comparable) (this.pages.get(m).getMinV()))) >= 0) {
				return m;
			}
			if (m > 0 && ((((Comparable) target).compareTo((Comparable) (this.pages.get(m - 1).getMaxV()))) > 0
					&& (((Comparable) target).compareTo((Comparable) (this.pages.get(m).getMinV()))) < 0)) {
				if (this.pages.get(m).isFull() && !this.pages.get(m - 1).isFull())
					return m - 1;
				else
					return m;
			}
			if ((((Comparable) target).compareTo((Comparable) (this.pages.get(m).getMaxV()))) > 0) {
				l = m + 1;
			} else
				r = m - 1;
		}
		return -1;
	}

	private Object parseType(String clusteringKeyValue) {
		Object o = null;
		if (type.equals("java.lang.Integer"))
			o = Integer.parseInt(clusteringKeyValue);
		else if (type.equals("java.lang.String"))
			o = clusteringKeyValue;
		else if (type.equals("java.lang.Double"))
			o = Double.parseDouble(clusteringKeyValue);
		else if (type.equals("java.util.Date"))
			try {
				o = new SimpleDateFormat("yyyy-MM-dd").parse(clusteringKeyValue);
			} catch (ParseException e) {
				System.out.println("Invalid Date");
			}

		return o;

	}

	private void delectPageFile(Page p) {
		File myFile = new File(p.getPath());
		myFile.delete();
	}

	private int getNumofRow() {
		try (FileReader reader = new FileReader("src/main/resources/DBApp.config")) {
			Properties properties = new Properties();
			properties.load(reader);
			String s = properties.getProperty("MaximumRowsCountinPage");
			return Integer.parseInt(s);

		} catch (Exception e) {
			;
			e.printStackTrace();
		}
		return -1;

	}

	public Vector<Page> getPages() {

		return pages;
	}

	public String getTableName() {
		return tableName;
	}

	public int getNumOfGrids() {
		return NumOfGrids;
	}

	public String getClusteringKey() {
		return clusteringKey;
	}

	//// print for testing
	public void print() {
		int count = 0;
		for (int i = 0; i < this.pages.size(); i++) {
			System.out.println(this.pages.get(i));

		}
		System.out.println(count);
	}

	public void createIndex(String[] columnNames) {
		Grid g = new Grid(columnNames, this);
		this.NumOfGrids++;
		this.grids.add(g);
		//System.out.println(this.grids.size()+" "+tableName +" ddd"+" "+NumOfGrids);
		

	}

}
