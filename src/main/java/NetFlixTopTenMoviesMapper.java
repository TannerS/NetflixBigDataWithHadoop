// http://www.drdobbs.com/database/hadoop-writing-and-running-your-first-pr/240153197
// http://stackoverflow.com/questions/11086263/understanding-longwritable
// http://stackoverflow.com/questions/11122832/hadoop-mapreduce-possible-to-define-two-mappers-and-reducers-in-one-hadoop-job
// http://stackoverflow.com/questions/14922087/hadoop-longwritable-cannot-be-cast-to-org-apache-hadoop-io-intwritable
// http://stackoverflow.com/questions/17262188/type-mismatch-in-key-from-map-expected-org-apache-hadoop-io-text-recieved-org

/*

    Prompt:

    What are the top 10 movies that have the highest average ratings?
    Tell us the titles (you can find the titles in a separated file

    Input:

    TrainingRatings.txt

    Description:

    This dataset is a subset of the data provided as part of the Netflix
    Prize. TrainingRatings.txt has lines having the format:
    MovieID,UserID,Rating.

    Format:

    MovieID,UserID,Rating.

    e.g. 7067,2323490,3.0

    Strategy is to read in each line as a Text and split it by 3, using the movie id as a key, and rating as value.

    The mapper would so something like this

    ("dobbs", 20)
    ("dobbs", 22)
    ("doctor", 545525)
    ("doctor", 668666)

    this is how the data will be writting using context.write()

    what about the top ten? or the avg? This is done in the reducer that will see the data as

    ("dobbs", [20, 22])
    ("doctor", [545525, 668666])

 */

import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
    public class Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>

    Data will be representeded as a key value pair. the parameters of Mapper will represent
    the input/output key/value pair types

    In our program, we will use classes that are wrappers that are part of hadoop to be
    able to accomplish serialization

 */


// for a mapper classes, the key is always LongWritable, but you can choose the outut to be
// LongWritable = always this, see link above,
// Text = our value is a text, each row of the file is a text,
// Text = returning key as a text, i assume it would also work as int,
// FloatWritable = needed for average, I think it is ok keeping this as float since later we need to do avg which we cant as a Text
public class NetFlixTopTenMoviesMapper extends Mapper<LongWritable, Text, Text, FloatWritable>{

    /*
         With two instance variables, the map() method is called once per input record, so it pays to avoid unnecessary object creation.
     */
    private Text new_key = new Text();
    private FloatWritable new_value = new FloatWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // split row up by comma (see input format above)
        String[] output =  value.toString().split(",");
        // 0 index ia movie id, 1 is user id (not needed for this job), 2 index is the rating

//        System.out.println("KEY: " + output[0] + " value: " + output[2]);

        new_key.set(output[0]);
        new_value.set(Float.valueOf(output[2]));
        context.write(new_key, new_value);
    }
}

