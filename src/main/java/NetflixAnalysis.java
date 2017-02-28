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
import org.apache.hadoop.util.ToolRunner;

import java.io.*;
import java.util.*;

public class NetflixAnalysis  extends Configured implements Tool
{
    public static void main(String[] args)
    {
        int size = 10;

        try
        {
            int status = ToolRunner.run(new NetflixAnalysis(), args);
//
            int[] movies = getTopTenMovies("txt/TopTenMoviesSorted/part-r-00000");
            HashMap<Integer, String> titles = loadTitles("txt/movie_titles.txt");

            System.out.println("Top Movies");

            for(int i = 0 ;i < movies.length; i++) {
                System.out.println((i + 1) + " " + titles.get(movies[i]));
            }

            titles.clear();

            int[] users = getTopTenUsers("txt/TopTenUsersSorted/part-r-00000");

            System.out.println("Top Users");

            for(int i = 0 ;i < size; i++) {
                System.out.println((i + 1) + " " + users[i]);
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            System.exit(-1);
        }
    }

//    private static TreeMap<Integer, Integer> getTopTenUsers(String path)
//    {
//        BufferedReader file_reader = null;
//        TreeMap<Integer, Integer> data = new TreeMap<>(Collections.reverseOrder());
//
//        try
//        {
//            file_reader = new BufferedReader(new FileReader(new File(path)));
//
//            String line;
//
//            String[] movies = null;
//
//            while((line = file_reader.readLine()) != null)
//            {
//
//                movies = line.split("\t");
//                System.out.println(movies[1] + " " + movies[0]);
//
//                data.put(Integer.parseInt(movies[1]), Integer.parseInt(movies[0]));
//
//                System.out.println(data.get(Integer.parseInt(movies[1])));
//
//
//            }
//        }
//        catch (FileNotFoundException e)
//        {
//            e.printStackTrace();
//        }
//        catch (IOException e)
//        {
//            e.printStackTrace();
//        }
//        finally
//        {
//            if (file_reader != null)
//            {
//                try
//                {
//                    file_reader.close();
//                }
//                catch (IOException e)
//                {
//                    e.printStackTrace();
//                }
//            }
//        }
//
//        return data;
//    }

    private static int[] getTopTenUsers(String path)
    {
        BufferedReader file_reader = null;
        int[] data = new int[10];

        try
        {
            file_reader = new BufferedReader(new FileReader(new File(path)));

            String line;

            String[] users = null;

            int count = 0;

            while((line = file_reader.readLine()) != null)
            {
                if(count == 10)
                    break;
                users = line.split("\t");
                data[count] = Integer.parseInt(users[1]);
                count++;
            }
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

    private static int[] getTopTenMovies(String path)
    {
        BufferedReader file_reader = null;

        int[] data = new int[10];

        try
        {
            file_reader = new BufferedReader(new FileReader(new File(path)));

            String line;

            String[] movies = null;

            int count = 0;

            while((line = file_reader.readLine()) != null)
            {
                if(count == 10)
                    break;
                movies = line.split("\t");
                data[count] = (Integer.parseInt(movies[1]));
                count++;

            }
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
            file_reader = new BufferedReader(new FileReader(new File(path)));

            titles = new HashMap<Integer, String>();

            String line;

            while((line = file_reader.readLine()) != null)
            {
                String[] parts_of_title = line.split(",", 3);
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
//        System.out.println("***********************************************************" + top_ten_movies_job.getWorkingDirectory());

        if(top_ten_movies_job.isSuccessful())
        {
            System.out.println("Top Ten Movie Job Was Successful");

            Job top_ten_movies_sorter_job = new Job();
            top_ten_movies_sorter_job.setJarByClass(NetflixAnalysis.class);
            top_ten_movies_sorter_job.setJobName("NetflixTopMoviesAnalysisSorter");
            FileInputFormat.addInputPath(top_ten_movies_sorter_job, new Path("txt/TopTenMovies/part-r-00000"));
            FileOutputFormat.setOutputPath(top_ten_movies_sorter_job, new Path("txt/TopTenMoviesSorted"));
            top_ten_movies_sorter_job.setSortComparatorClass(FloatSortDesc.class);
            top_ten_movies_sorter_job.setOutputKeyClass(FloatWritable.class);
            top_ten_movies_sorter_job.setOutputValueClass(IntWritable.class);
            top_ten_movies_sorter_job.setOutputFormatClass(TextOutputFormat.class);
            top_ten_movies_sorter_job.setMapperClass(NetFlixTopTenMoviesMapperSorter.class);
//            top_ten_movies_sorter_job.setReducerClass(NetFlixTopTenMoviesReducerSorter.class);
            int top_movies_sorter_status_flag = top_ten_movies_sorter_job.waitForCompletion(true) ? 0:1;

            if(top_ten_movies_sorter_job.isSuccessful())
            {
                System.out.println("Top Ten Movie Sorter Job Was Successful");

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
                    System.out.println("Top Ten Users Job Was Successful");

                    Job top_ten_users_sorter_job = new Job();
                    top_ten_users_sorter_job.setJarByClass(NetflixAnalysis.class);
                    top_ten_users_sorter_job.setJobName("NetflixTopUsersSorterAnalysis");
                    FileInputFormat.addInputPath(top_ten_users_sorter_job, new Path("txt/TopTenUsers/part-r-00000"));
                    FileOutputFormat.setOutputPath(top_ten_users_sorter_job, new Path("txt/TopTenUsersSorted"));
                    top_ten_users_sorter_job.setSortComparatorClass(IntSortDesc.class);
                    top_ten_users_sorter_job.setOutputKeyClass(IntWritable.class);
                    top_ten_users_sorter_job.setOutputValueClass(IntWritable.class);
                    top_ten_users_sorter_job.setOutputFormatClass(TextOutputFormat.class);
                    top_ten_users_sorter_job.setMapperClass(NetFlixTopTenUsersMapperSorter.class);
                    int top_user_sorter_status_flag = top_ten_users_sorter_job.waitForCompletion(true) ? 0:1;

                    if(top_ten_users_job.isSuccessful())
                    {
                        System.out.println("Top Ten Users Sorter Job Was Successful");
                    }
                    else if(!top_ten_users_job.isSuccessful())
                    {
                        System.out.println("Top Ten Users Sorter Job Was Successful");
                    }
                } else if(!top_ten_users_job.isSuccessful()) {
                        System.out.println("Top Ten Users Job Was Not Successful");
                    }
            } else if(!top_ten_movies_sorter_job.isSuccessful()) {
                System.out.println("Top Ten Movie Sorter Job Was Not Successful");
            }
        } else if(!top_ten_movies_job.isSuccessful()) {
            System.out.println("Top Ten Movie Job Was Not Successful");
        }

        return 0;

    }

}
