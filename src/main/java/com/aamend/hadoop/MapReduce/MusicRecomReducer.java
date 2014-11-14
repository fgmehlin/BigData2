package com.aamend.hadoop.MapReduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MusicRecomReducer extends
    Reducer<Text, IntWritable, Text, IntWritable> {

  public void reduce(Text song, Iterable<IntWritable> values,
      Context context) throws IOException, InterruptedException {

    int sum = 0;
    System.out.println("----");
    for (IntWritable next : values) {
      sum += next.get();
    }

    System.out.println("sum : " + sum);
    context.write(song, new IntWritable(sum));

  }

}
