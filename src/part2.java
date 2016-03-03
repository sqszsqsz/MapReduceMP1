/**
 * Created by SweetSoul on 3/2/2016.
 */

import java.io.IOException;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.ArrayList;

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


public class part2 extends Configured implements Tool{

    public static class Map1 extends Mapper<Object, Text, Text, Text>
    {
        @Override
        public void map(Object key, Text value, org.apache.hadoop.mapreduce.Mapper.Context output) throws IOException, InterruptedException {

            String[] words = value.toString().split(",");
            Text map_val = new Text(words[0]+','+words[1]+','+words[3]);
            output.write(new Text(words[1]),map_val);
        }

    }

    public static class Reduce1 extends Reducer<Text, Text, Text, Text>
    {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context output) throws IOException, InterruptedException {

            for (Text value : values) {
                String[] words = value.toString().split(",");

                if (Integer.parseInt(words[2])==1) {
                    Text reduce_out = new Text(words[0]);
                    output.write(key,reduce_out);
                }
            }
        }

    }

    public static class Map2 extends Mapper<Object, Text, Text, Text>
    {
        @Override
        public void map(Object key, Text value, org.apache.hadoop.mapreduce.Mapper.Context output) throws IOException, InterruptedException {

            String[] words = value.toString().split(",");
            Text map_val = new Text(words[0]+","+words[1]);
            if (words[0].length()!= 0)
                output.write(new Text("Tuple"),map_val);
        }

    }

    public static class Reduce2 extends Reducer<Text, Text, Text, Text>
    {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context output) throws IOException, InterruptedException {

            ArrayList valuelist = new ArrayList();
            for (Text value : values) {
                valuelist.add(value.toString());
            }

            for (int i = 0; i < valuelist.size(); i++) {
                String[] outer_words = valuelist.get(i).toString().split(",");
                String outer_ID = outer_words[0];
                String outer_time = outer_words[1];

                for (int j = 0; j < valuelist.size(); j++) {
                    String[] inner_words = valuelist.get(j).toString().split(",");
                    String inner_ID = inner_words[0];
                    String inner_time = inner_words[1];

//                    System.out.println("out " + outer_ID + " inner " + inner_ID);

                    Timestamp ts1 = Timestamp.valueOf(outer_time);
                    Timestamp ts2 = Timestamp.valueOf(inner_time);
                    int diff = (int) Math.abs(ts1.getTime() - ts2.getTime()) / 1000;
//                    System.out.println("time diff=" + diff);

                    BigInteger outer_int = new BigInteger(outer_ID);
                    BigInteger inner_int = new BigInteger(inner_ID);

                    if (outer_int.compareTo(inner_int) == -1 && diff < 2) {
                        output.write(new Text(outer_time), new Text(outer_ID + "," + inner_ID));
                    }
                }
            }
        }
    }


    public static void main(String[] args) throws Exception
    {
        int res = ToolRunner.run(new Configuration(), new part2(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");

        if (args.length != 3) {
            System.err.println("Usage: Driver <in> <mid> <out>");
            System.exit(2);
        }

        // Job 1 - find all tasks which have click == 1
        Job job1 = Job.getInstance(conf);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        job1.setMapperClass(Map1.class);
        job1.setReducerClass(Reduce1.class);

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);


        FileInputFormat.setInputPaths(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        job1.setJarByClass(part2.class);
        job1.waitForCompletion(true);


        // Job2 find the required query results
        Job job2 = Job.getInstance(conf);
        job2.setJarByClass(part2.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        job2.setMapperClass(Map2.class);
        job2.setReducerClass(Reduce2.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(job2, new Path(args[1]));
        TextOutputFormat.setOutputPath(job2, new Path(args[2]));

        return job2.waitForCompletion(true) ? 0 : 1;
    }

}
