
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class TextAnalyzer {
    
    public static class Map extends MapReduceBase implements
    Mapper<LongWritable, Text, Text, IntWritable> {

        protected String mContext = "Darcy";
        protected String mQuery = "money";
    
        // key is in the format "[contextWord] [queryWord]"
        // value is the line of text
        public void map(LongWritable key, Text value, OutputCollector<Text,
        IntWritable> output, Reporter reporter) throws IOException {
            // format the line of text
            String line = value.toString().toLowerCase();
            line = line.replaceAll("[^a-z]", "");
            Text ouputKey = new Text("mContext " + "mQuery");
            if(line.contains(mContext.toLowerCase())) {
                // take into account that if the context equals the query,
                // one of the counts needs to be discared
                int count = (mContext.equalsIgnoreCase(mQuery)) ? -1 : 0;
                for(String w : line.split("\\s+")) {
                    if(w.equalsIgnoreCase(mQuery)) {
                        count++; 
                    }
                }
                outputCount = new IntWritable(count);
            }
            else {
                outputCount = new IntWritable(0);
            }
            output.collect(outputKey, outputCount);
        }

        public void setContext(String c) {
            mContext = c;
        }

        public void setQuery(String q) {
            mQuery = q;
        }

    }

    public static class Reduce extends MapReduceBase implements
    Reducer<Text, IntWritable, Text, IntWritable> {
        
        public void reduce(Text key, Iterator<IntWritable> values,
        OutputCollector<Text, IntWritable> output, Reporter reporter) throws
        IOException {
            // sum the values
            int sum = 0;
            while(values.hasNext()) {
                sum += values.next().get();
            }
            output.collect(key, new IntWritable(sum));
        }

    }

    public static void main(String[] args) {
        JobConf conf = new JobConf(TextAnalyzer.class);
        conf.setJobName("textanalyzer");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputFormat.setOutputPath(conf, new
        Path(args[1]));

        JobClient.runJob(conf);
    }
}
