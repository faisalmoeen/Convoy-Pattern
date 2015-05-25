package mapreduce.datagen;

public class Tuple {
	public Tuple() {
		super();
		
	}
	int oid;
	int t;
	double lati;
	double longi;
	@Override
	public String toString() {
		return oid + "," + t + "," + lati + ","+ longi;
	}
	
}
