import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class ReduceSideJoinc {
    // Map1 will emit the <userid>,<friends of userid> as key,value
    public static class Map1 extends Mapper<LongWritable, Text, Text, Text> {
        private Text emit_key = new Text();
        private Text emit_value = new Text();

        public void map(LongWritable key, Text val, Context context) throws IOException, InterruptedException {
            // System.out.println("Entering MAP1\n");
            String[] mydata = val.toString().split("\\t");
            if (mydata.length <= 1) return;
            String friends = mydata[1];

            emit_key.set(mydata[0]);
            emit_value.set(friends);
            context.write(emit_key, emit_value);
        }
    }

    // Map 2 will emit the <userid>,<userdetail of userid> as key,value
    public static class Map2 extends Mapper<LongWritable, Text, Text, Text> {
        private Text emit_key = new Text();
        private Text emit_value = new Text();

        public void map(LongWritable key, Text val, Context context) throws IOException, InterruptedException {
            //System.out.println("Entering MAP2\n");
            String[] line = val.toString().split(",");
            if (line.length != 10) return;
            String address = line[3] + "," + line[4] + "," + line[5];
            //System.out.println("address in map is : " + address);

            emit_key.set(line[0]);
            emit_value.set(address);
            context.write(emit_key, emit_value);
        }
    }

    // ReduceJoin reducer will implement reduce side join based on user id's
    public static class ReduceJoin extends Reducer<Text, Text, Text, Text> {
        private Text emit_key = new Text();
        private Text emit_value = new Text();
        HashMap<String, String> map1 = new HashMap<>();

        @Override
        protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException {
            Configuration confg = new Configuration();
            FileSystem fs = FileSystem.get(confg);
            URI[] loc = context.getCacheFiles();
            Path path = new Path(loc[0].getPath());
            read_file(fs.open(path));
        }

        void read_file(FSDataInputStream path) throws IOException {
            BufferedReader buff_rdr = new BufferedReader(new InputStreamReader(path));
            String line = buff_rdr.readLine();
            while (line != null) {0
                String[] usr_data = line.split(",");
                if (usr_data.length == 10) {
                    map1.put(usr_data[0].trim(), usr_data[9]);
                }
                line = buff_rdr.readLine();
            }
        }

        int findAge(String u_id) {
            if (map1.containsKey(u_id)) {
                String[] DOB = map1.get(u_id).split("/");                  // Date of birth: MM/DD/YYYY
                int yob = Integer.parseInt(DOB[2]);                         // Year of birth
                // System.out.println("YOB is: " + yob);
                return (2019 - yob);
            } else return 0;
        }

        public void reduce(Text key, Iterable<Text> val, Context context) throws IOException, InterruptedException {
            //System.out.println("Entering Reduce in ReduceJoin\n");
            String[] red_input = new String[2];
            int i = 0;
            for (Text value : val) {
                red_input[i++] = value.toString();
            }
            if (red_input.length != 2) return;
            String[] friends = red_input[0].split(",");
            String address = red_input[1];
            int age_sum = 0;

            for (String friend : friends) {
                age_sum = age_sum + findAge(friend);
            }
            int avg_age = age_sum / (friends.length);
            emit_key.set(key);                                              // Output key is userid
            emit_value.set(Integer.toString(avg_age) + ";" + address);       // Output value is "average age:address of userid"
            context.write(emit_key, emit_value);
        }
    }

    // Mapper of job2 will output <average age>,<userid, address of userid> as <key>,<value>
    public static class MapJob2 extends Mapper<LongWritable, Text, IntWritable, Text> {
        private IntWritable emit_key = new IntWritable();
        private Text emit_val = new Text();

        public void map(LongWritable key, Text val, Context context) throws IOException, InterruptedException {
            String[] map2_ip = val.toString().split("\\t");
            String u_id = map2_ip[0];
            String[] usr_info = map2_ip[1].split(";");
            int avg_age = Integer.parseInt(usr_info[0]);
            String address = usr_info[1];
            /*
            String usr_info = map2_ip[1];
            int avg_age = Integer.parseInt(usr_info.split(":")[0]);
            String address = usr_info.split(":")[1];
            */

            emit_key.set(avg_age);
            emit_val.set(u_id + ":" + address);
            context.write(emit_key, emit_val);
        }
    }

    // After sortComparator sorts the output from MapJob2 in descending order of "average age", ReduceJob2 reducer will
    // output the <userID>,<address, average age>
    public static class ReduceJob2 extends Reducer<IntWritable, Text, Text, Text> {
        private Text emit_key = new Text();
        private Text emit_value = new Text();

        public void reduce(IntWritable key, Iterable<Text> val, Context context) throws IOException, InterruptedException {
            if (key.get() == 0) return;
            List<String> usr_address = new ArrayList<>();
            for (Text value : val) {
                usr_address.add(value.toString());
            }

            String uid_add_avgAge = "";
            for (String p : usr_address) {
                String[] usrID_add = p.split(":");
                String u_id = usrID_add[0];
                uid_add_avgAge += u_id + "," + usrID_add[1] + "," + key.toString() + "\n";
            }

            emit_key.set(uid_add_avgAge);
            context.write(emit_key, emit_value);
        }
    }


    public static class Comparator_decreasing extends WritableComparator {
        public Comparator_decreasing() {
            super(IntWritable.class, true);
        }

        @Override
        public int compare(WritableComparable obj1, WritableComparable obj2) {
            IntWritable o1 = (IntWritable) obj1;
            IntWritable o2 = (IntWritable) obj2;
            return -1 * o1.compareTo(o2);

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf1 = new Configuration();
        String[] otherargs = new GenericOptionsParser(conf1, args).getRemainingArgs();
        if (otherargs.length != 5) {
            System.err.println(otherargs[0]);
            System.err.println("Incorrect arguments passed!");
            System.exit(2);
        }

        Job job1 = new Job(conf1, "FirstMR");            // first mapreduce job
        job1.addCacheFile(new Path(otherargs[2]).toUri());
        job1.setJarByClass(ReduceSideJoinc.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job1, new Path(otherargs[1]), TextInputFormat.class, Map1.class);
        MultipleInputs.addInputPath(job1, new Path(otherargs[2]), TextInputFormat.class, Map2.class);
        job1.setReducerClass(ReduceJoin.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        //FileInputFormat.addInputPath(job1, new Path(otherargs[1]));
        FileOutputFormat.setOutputPath(job1, new Path(otherargs[3]));
        boolean result1 = job1.waitForCompletion(true);
        // System.exit(job1.waitForCompletion(true) ? 0 : 1);

        if (result1) {
            Configuration conf2 = new Configuration();
            Job job2 = new Job(conf2, "SecondMR");       // second mapreduce job
            job2.setJarByClass(ReduceSideJoinc.class);
            FileInputFormat.addInputPath(job2, new Path(args[3]));
            job2.setMapperClass(MapJob2.class);
            job2.setSortComparatorClass(Comparator_decreasing.class);
            job2.setReducerClass(ReduceJob2.class);

            job2.setMapOutputKeyClass(IntWritable.class);
            job2.setMapOutputValueClass(Text.class);

            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            FileOutputFormat.setOutputPath(job2, new Path(otherargs[4]));

            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }
    }
}
