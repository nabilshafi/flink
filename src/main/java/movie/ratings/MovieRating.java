package movie.ratings;

import org.apache.flink.api.java.tuple.Tuple6;

public class MovieRating extends Tuple6<String,String,String,String,String,String> {


    public MovieRating(){

    }

    public MovieRating(String userId,String movieId,String rating,String timestamp, String time1,String time2){

        super(userId,movieId,rating,timestamp,time1,time2);

    }

}
