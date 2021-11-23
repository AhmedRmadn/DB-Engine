
public class SQLTerm {
	String _strTableName ;
	String _strColumnName;
	String _strOperator ;
	Object _objValue;
	
	public SQLTerm() {
		
	}
	
	public boolean sat(Object o) {
		
		boolean f = false;
		if(_strOperator.equals("!=")) 
			f =((Comparable)o).compareTo(_objValue)!=0;
		else if(_strOperator.equals("<")) 
			f= ((Comparable)o).compareTo(_objValue)<0;			
		else if(_strOperator.equals("<=")) 
			f= ((Comparable)o).compareTo(_objValue)<=0;
		else if(_strOperator.equals(">")) 
			f= ((Comparable)o).compareTo(_objValue)>0;	
		else if(_strOperator.equals(">=")) 
			f= ((Comparable)o).compareTo(_objValue)>=0;		
		else if(_strOperator.equals("=")) {
			f= ((Comparable)o).compareTo(_objValue)==0;
			//System.out.println(this+" "+o+" "+f);
			}
		
		
         return f;

	}
	
	public boolean validTerm() {
		return this._strOperator.equals("=")||this._strOperator.equals("<")||this._strOperator.equals("<=")||this._strOperator.equals(">")||this._strOperator.equals(">=")||this._strOperator.equals("!=");
		
	}

	@Override
	public String toString() {
		return "SQLTerm [_strTableName=" + _strTableName + ", _strColumnName=" + _strColumnName + ", _strOperator="
				+ _strOperator + ", _objValue=" + _objValue + "]";
	}
	
}
