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



    public static class Map extends Mapper<Object, Text, Text, MapWritable>
    {
        MapWritable map_out = new MapWritable();
        Text textKey = new Text();

        @Override
        public void map(Object key, Text value, org.apache.hadoop.mapreduce.Mapper.Context output) throws IOException, InterruptedException {

            String[] words = value.toString().split(",");
            map_out.put(new IntWritable(1),new IntWritable(Integer.parseInt(words[2])));
            map_out.put(new IntWritable(2),new IntWritable(Integer.parseInt(words[3])));
            map_out.put(new IntWritable(3),new IntWritable(Integer.parseInt(words[4])));
            textKey.set(words[5]);

            System.out.println(words[0]+' '+words[1]+' '+words[2]+' '+words[3]+' '+words[4]+' '+words[5]);

            output.write(textKey,map_out);
        }

    }

    public static class Reduce extends Reducer<Text, MapWritable, Text, Text>
    {
        Text reduce_out = new Text();

        @Override
        public void reduce(Text key, Iterable<MapWritable> values, Context output) throws IOException, InterruptedException {

            int impression = 0, click = 0, conversion = 0;

            for (MapWritable value : values) {
                int i = ((IntWritable) value.get(new IntWritable(1))).get();
                int j = ((IntWritable) value.get(new IntWritable(2))).get();
                int k = ((IntWritable) value.get(new IntWritable(3))).get();

                impression = impression+i;
                click = click + j;
                conversion = conversion + k;
            }

            reduce_out.set(impression+"\t"+click+"\t"+conversion);
            System.out.println(reduce_out);
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

        Job job = Job.getInstance(new Configuration());
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
