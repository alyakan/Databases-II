public class Record implements java.io.Serializable {
	private String value; // Data inside a record
	private int id; // Table-specific ID .. Yet to be used
	private static int idGen = 0; // Global Identification number
	
	public Record(String value) {
		this.setValue(value);
		setId(idGen);
		idGen ++;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}
	
}
