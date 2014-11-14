package com.aamend.hadoop.MapReduce;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MusicRecom {

  public static void main(String args[]) throws IOException,
      InterruptedException, ClassNotFoundException {

    Path inputPath = new Path(args[0]);
    Path outputDir = new Path(args[1]);
    String user = args[2];

    BufferedReader br = null;
    String line = "";
    String[] lineSplit;
    ArrayList<String> playlist = new ArrayList<String>();

    try {
      br =
          new BufferedReader(
              new FileReader(
                  new File(
                      "/Volumes/Data/ETHZ/Big Data/year1_valid_triplet_visible_proc.txt")));

      while ((line = br.readLine()) != null) {
        lineSplit = line.split(",");
        if (lineSplit[0].equals(user)) {
          playlist.add(lineSplit[1]);
        }
      }


    } catch (IOException e) {
      e.printStackTrace();
    }

    String[] songs = new String[playlist.size()];

    for (int i = 0; i < playlist.size(); i++) {
      songs[i] = playlist.get(i);
    }


    // Create configuration
    Configuration conf = new Configuration(true);
    conf.set("user", user);
    conf.setStrings("songs", songs);

    // Create job
    Job job = new Job(conf, "MusicRecom");
    job.setJarByClass(MusicRecomMapper.class);

    // Setup MapReduce
    job.setMapperClass(MusicRecomMapper.class);
    job.setReducerClass(MusicRecomReducer.class);
    job.setNumReduceTasks(1);

    // Specify key / value
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    // Input
    FileInputFormat.addInputPath(job, inputPath);
    job.setInputFormatClass(TextInputFormat.class);

    // Output
    FileOutputFormat.setOutputPath(job, outputDir);
    job.setOutputFormatClass(TextOutputFormat.class);

    // Delete output if exists
    FileSystem hdfs = FileSystem.get(conf);
    if (hdfs.exists(outputDir))
      hdfs.delete(outputDir, true);

    // Execute job
    int code = job.waitForCompletion(true) ? 0 : 1;


    System.exit(code);

  }

}
