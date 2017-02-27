import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.*;
import java.util.*;

public class NetflixAnalysis  extends Configured implements Tool
{
    public static void main(String[] args)
    {
        int size = 10;
        int count = 0;

        try
        {
//            int status = ToolRunner.run(new NetflixAnalysis(), args);

            TreeMap<Float, Integer> movies = getMovies("txt/TopTenMovies/part-r-00000");
            HashMap<Integer, String> titles = loadTitles("txt/movie_titles.txt");

            int[] top_movies = new int[size];

            for (Float key : movies.keySet())
            {
                if(count == size)
                    break;
                top_movies[count] = movies.get(key);
                count++;
//                System.out.println("FINAL DATA: " + key + " value: " + movies.get(key));
            }



            System.out.println("Top Movies");

            for(int i = 0 ;i < size; i++) {
                System.out.println((i + 1) + " " + titles.get(top_movies[i]));
            }

            movies.clear();
            titles.clear();





//            for(int i = 0 ;i < size; i++)
//            {
//                System.out.println("TOP: " + top_movies[i]);
//            }


//


//

            TreeMap<Integer, Integer> users = getUsers("txt/TopTenUsers/part-r-00000");

            int[] top_users = new int[size];


            count = 0;



            for (Integer key : users.keySet())
            {
                if(count == size)
                    break;
                top_users[count] = users.get(key);
                count++;
//                System.out.println("FINAL DATA: " + key + " value: " + users.get(key));
            }

            System.out.println("Top Users");

            for(int i = 0 ;i < size; i++) {
                System.out.println((i + 1) + " " + top_users[i]);
            }



        }
        catch (Exception e)
        {
            e.printStackTrace();
            System.exit(-1);
        }




    }

    private static TreeMap<Integer, Integer> getUsers(String path)
    {
        BufferedReader file_reader = null;
        TreeMap<Integer, Integer> data = new TreeMap<>(Collections.reverseOrder());

        try
        {
            file_reader = new BufferedReader(new FileReader(new File(path)));

            String line;

            String[] movies = null;

            while((line = file_reader.readLine()) != null)
            {

                movies = line.split("\t");
                System.out.println(movies[1] + " " + movies[0]);

                data.put(Integer.parseInt(movies[1]), Integer.parseInt(movies[0]));

                System.out.println(data.get(Integer.parseInt(movies[1])));


            }
        }
        catch (FileNotFoundException e)
        {
            e.printStackTrace();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        finally
        {
            if (file_reader != null)
            {
                try
                {
                    file_reader.close();
                }
                catch (IOException e)
                {
                    e.printStackTrace();
                }
            }
        }

        return data;
    }

    private static TreeMap<Float, Integer> getMovies(String path)
    {
        BufferedReader file_reader = null;
        TreeMap<Float, Integer> data = new TreeMap<>(Collections.reverseOrder());

        try
        {
            file_reader = new BufferedReader(new FileReader(new File(path)));

            String line;

            String[] movies = null;

            while((line = file_reader.readLine()) != null)
            {
                movies = line.split("\t");
                data.put(Float.parseFloat(movies[1]), Integer.parseInt(movies[0]));
            }
        }
        catch (FileNotFoundException e)
        {
            e.printStackTrace();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        finally
        {
            if (file_reader != null)
            {
                try
                {
                    file_reader.close();
                }
                catch (IOException e)
                {
                    e.printStackTrace();
                }
            }
        }

        return data;
    }

    private static HashMap<Integer, String> loadTitles(String path)
    {
        HashMap<Integer, String> titles = null;
        BufferedReader file_reader = null;

        try
        {
//            file_reader = new BufferedReader(new FileReader(new File("txt/movie_titles.txt")));
            file_reader = new BufferedReader(new FileReader(new File(path)));

            titles = new HashMap<Integer, String>();

            String line;

            while((line = file_reader.readLine()) != null)
            {
                // split file line using comma as a delimiter
                String[] parts_of_title = line.split(",", 3);

                System.out.println(Arrays.toString(parts_of_title));
                System.out.println(parts_of_title[0]);
                System.out.println(parts_of_title[1]);

                // insert into hashmap the key as index 0 and value as index 2
//                System.out.println("ID: " + parts_of_title[0] + " TITLE: " + parts_of_title[2] );
                titles.put(Integer.parseInt(parts_of_title[0]), parts_of_title[2]);
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        finally
        {
            if (file_reader != null) {
                try {
                    file_reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        return titles;
    }

    public int run(String[] args) throws Exception {

        Job top_ten_movies_job = new Job();
        top_ten_movies_job.setJarByClass(NetflixAnalysis.class);
        top_ten_movies_job.setJobName("NetflixTopMoviesAnalysis");
        FileInputFormat.addInputPath(top_ten_movies_job, new Path("txt/TrainingRatings.txt"));
        FileOutputFormat.setOutputPath(top_ten_movies_job, new Path("txt/TopTenMovies"));
        top_ten_movies_job.setOutputKeyClass(Text.class);
        top_ten_movies_job.setOutputValueClass(FloatWritable.class);
        top_ten_movies_job.setOutputFormatClass(TextOutputFormat.class);
        top_ten_movies_job.setMapperClass(NetFlixTopTenMoviesMapper.class);
        top_ten_movies_job.setReducerClass(NetFlixTopTenMoviesReducer.class);
        int top_movies_status_flag = top_ten_movies_job.waitForCompletion(true) ? 0:1;

        if(top_ten_movies_job.isSuccessful()) {
            System.out.println("Job was successful");
        } else if(!top_ten_movies_job.isSuccessful()) {
            System.out.println("Job was not successful");
        }

//        return top_movie_status_flag;

        Job top_ten_users_job = new Job();
        top_ten_users_job.setJarByClass(NetflixAnalysis.class);
        top_ten_users_job.setJobName("NetflixTopUsersAnalysis");
        FileInputFormat.addInputPath(top_ten_users_job, new Path("txt/TrainingRatings.txt"));
        FileOutputFormat.setOutputPath(top_ten_users_job, new Path("txt/TopTenUsers"));
        top_ten_users_job.setOutputKeyClass(Text.class);
        top_ten_users_job.setOutputValueClass(IntWritable.class);
        top_ten_users_job.setOutputFormatClass(TextOutputFormat.class);
        top_ten_users_job.setMapperClass(NetFlixTopTenUsersMapper.class);
        top_ten_users_job.setReducerClass(NetFlixTopTenUsersReducer.class);
        int top_user_status_flag = top_ten_users_job.waitForCompletion(true) ? 0:1;

        if(top_ten_users_job.isSuccessful()) {
            System.out.println("Job was successful");
        } else if(!top_ten_users_job.isSuccessful()) {
            System.out.println("Job was not successful");
        }

        int final_status = top_movies_status_flag & top_user_status_flag;


        return final_status;




    }

}
