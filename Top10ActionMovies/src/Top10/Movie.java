package Top10;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class Movie implements Comparable<Movie>, Writable{

	String id;
	String name;
	Double averageRating;
	
	//default constructor
	public Movie(){
		this.id = "";
		this.name = "";
		this.averageRating = 0.0;
	}
	
	//main constructor
	public Movie(String movie_id, String movie_name, Double avg_rating) {
		super();
		this.id = movie_id;
		this.name = movie_name;
		this.averageRating = avg_rating;
	}
	
	//copy constructor
	public Movie(Movie movie) {
		this.id = movie.id;
		this.name = movie.name;
		this.averageRating = movie.averageRating;
	}
	
	@Override
	public int compareTo(Movie movie) {
		// TODO Auto-generated method stub
		if(this.averageRating > movie.averageRating) {
			return 1;
		} else if (this.averageRating == movie.averageRating) {
			if (this.id == movie.id) {
				return 0;
			}
		} 
		return -1;
	}
	
	public String toString() {
		return "Movie Id > "+this.id+" "+"Name > "+this.name+" "+"Rating > "+this.averageRating;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(this.id);
		out.writeUTF(this.name);
		out.writeDouble(this.averageRating);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.id = in.readUTF();
		this.name = in.readUTF();
		this.averageRating = in.readDouble();
	}
}
