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

public class StudentPass {

    public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable> {

    // input will be as:
    // <STUDENT_ID> <COURSE_ID> <GRADE>
    private final static IntWritable one = new IntWritable(1);
    private Text studentId = new Text();
    private Text courseId = new Text();
    private Text grade = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      
      while (itr.hasMoreTokens()) {
        studentId.set(itr.nextToken());
        courseId.set(itr.nextToken());
        grade.set(itr.nextToken());

        if (Integer.parseInt(grade.toString()) >= 60) {
          context.write(studentId, one);
        }
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }
        
}