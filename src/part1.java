/**
 * Created by SweetSoul on 3/2/2016.
 */

// use winSCP to import files into hue

// Basic MR code was modified from the online tutorial
// http://hortonworks.com/wp-content/uploads/2012/03/Tutorial_Hadoop_HDFS_MapReduce.pdf
// and https://www.youtube.com/watch?v=4fzhAmnWlrg&feature=youtu.be

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class part1 extends Configured implements Tool{



    public static class Map extends Mapper<Object, Text, Text, Text>
    {
        @Override
        public void map(Object key, Text value, org.apache.hadoop.mapreduce.Mapper.Context output) throws IOException, InterruptedException {

            String[] words = value.toString().split(",");

            Text map_val = new Text(words[2]+','+words[3]+','+words[4]);

            output.write(new Text(words[5]),map_val);
        }

    }

    public static class Reduce extends Reducer<Text, Text, Text, Text>
    {


        @Override
        public void reduce(Text key, Iterable<Text> values, Context output) throws IOException, InterruptedException {

            int impression = 0, click = 0, conversion = 0;

            for (Text value : values) {
                String[] words = value.toString().split(",");

                impression = impression + Integer.parseInt(words[0]);
                click = click + Integer.parseInt(words[1]);
                conversion = conversion + Integer.parseInt(words[2]);

            }
            Text reduce_out = new Text(impression+","+click+","+conversion);

            output.write(key,reduce_out);
        }

    }


    public static void main(String[] args) throws Exception
    {

        int res = ToolRunner.run(new Configuration(), new part1(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("Input: yarn jar package.jar input output");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");

        Job job = Job.getInstance(conf);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        //File Input argument passed as a command line argument
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        //File Output argument passed as a command line argument
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setJarByClass(part1.class);

        job.submit();
        return 0;
    }

}
