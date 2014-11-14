package com.aamend.hadoop.MapReduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MusicRecomMapper extends
 Mapper<Object, Text, Text, IntWritable> {
  private Text song = new Text();

  public void map(Object key, Text value, Context context) throws IOException,
      InterruptedException {

    Configuration conf = context.getConfiguration();



    if (conf == null)
      System.exit(-1);
    String[] songsInput = conf.getStrings("songs");
    String[] csv = value.toString().split("\\s+");

    for (int i = 0; i < songsInput.length; i++) {
      if (songsInput[i].equals(csv[0]) && !csv[0].equals(csv[1])) {
        song.set(csv[1]);

        context.write(song, new IntWritable(Integer.parseInt(csv[2])));
      }
    }

  }
}
