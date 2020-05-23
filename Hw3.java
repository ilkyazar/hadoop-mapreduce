import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Hw3 {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "main");

        job.setJarByClass(Hw3.class);

        // input will be as:
        // <STUDENT_ID> <COURSE_ID> <GRADE>
        if (args[0].equals("cap")) {
            job.setMapperClass(StudentCount.TokenizerMapper.class);
            job.setCombinerClass(StudentCount.IntSumReducer.class);
            job.setReducerClass(StudentCount.IntSumReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);            
        }

        else if (args[0].equals("pass")) {
            job.setMapperClass(StudentPass.TokenizerMapper.class);
            job.setCombinerClass(StudentPass.IntSumReducer.class);
            job.setReducerClass(StudentPass.IntSumReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class); 
        }

        else if (args[0].equals("avg")) {
            job.setMapperClass(StudentAverage.TokenizerMapper.class);
            //job.setCombinerClass(StudentAverage.IntSumReducer.class);
            job.setReducerClass(StudentAverage.IntSumReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class); 
        }

        // output jar file will be tested as: hadoop jar Hw3.jar Hw3 cap input output_c
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
      }
}