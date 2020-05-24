import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CourseTwolist {

    public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable> {

    // input will be as:
    // <STUDENT_ID> <COURSE_ID> <GRADE>
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

        IntWritable gradeWritable = new IntWritable(Integer.parseInt(grade.toString())); 
        
        context.write(courseId, gradeWritable);
      }
    }
  }

  public static class AvgPartitioner
        extends Partitioner<Text,IntWritable> {
    public int getPartition(Text key, IntWritable value, int i) {
        System.out.println("\n\n" + "val:\n\n");
        System.out.println(value);
        if (value.get() >= 60)
            return 0;
        else
            return 1;
    }
}

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,DoubleWritable> {
    private DoubleWritable result = new DoubleWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      int studentCount = 0;
      for (IntWritable val : values) {
        sum += val.get();
        
        studentCount++;
      }

      double averageGrade = (double) sum / (double) studentCount;
      result.set(averageGrade);
      context.write(key, result);
    }
  }
        
}