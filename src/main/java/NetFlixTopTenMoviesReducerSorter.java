//import org.apache.hadoop.io.FloatWritable;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Reducer;
//
//import java.io.IOException;
//
///*
//    these inputs, must match the ones from the mapper class
// */
//
//public class NetFlixTopTenMoviesReducerSorter extends Reducer<FloatWritable, IntWritable, FloatWritable, IntWritable> {
//
//    private IntWritable id = new IntWritable();
//
//
//    public void reduce(FloatWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
//    {
//        // So loop all values and all them up, also keep count for avg division
//        for (IntWritable value : values) {
//            id.set(value.get());
//        }
//
//
//        // write result to file
//        context.write(key, id);
//    }
//
//}
