import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.*;

public class Topten_commFriends {
    public static class Map1 extends Mapper<LongWritable, Text, Text, Text> {
        private Text usr = new Text();
        private Text friend_list = new Text();

        public void map(LongWritable key, Text val, Context context) throws IOException, InterruptedException {
            String[] mydata = val.toString().split("\\t");
            if (mydata.length <= 1) return;
            String u_id = mydata[0];
            String[] friends = mydata[1].split(",");
            String emit_key;
            for (String friend : friends) {
                if (Integer.parseInt(u_id) > Integer.parseInt(friend)) {
                    emit_key = friend + "," + u_id;
                } else {
                    emit_key = u_id + "," + friend;
                }
                usr.set(emit_key);
                friend_list.set(mydata[1]);
                context.write(usr, friend_list);
            }
        }
    }

    public static class Reduce1 extends Reducer<Text, Text, Text, Text> {
        private Text common_frnds = new Text();

        public void reduce(Text key, Iterable<Text> val, Context context) throws IOException, InterruptedException {
            String[] lists = new String[2];
            int i = 0;

            // Store the lists of friends for <usrA, usrB> key pair
            for (Text list : val) {
                lists[i++] = list.toString();
            }

            String[] list1 = lists[0].split(",");
            String[] list2 = lists[1].split(",");
            Set<String> set1 = new HashSet<>(Arrays.asList(list1));
            Set<String> set2 = new HashSet<>(Arrays.asList(list2));
            set1.retainAll(set2);
            List<String> res = new ArrayList<>(set1);

            common_frnds.set(String.join(",", res));
            context.write(key, common_frnds);
        }
    }

    public static class Map2 extends Mapper<LongWritable, Text, IntWritable, Text> {
        private IntWritable key1 = new IntWritable();                    // output key will be the number of mutual friends of the pair
        private Text pair_friends = new Text();                  // output value will be friend-pair & their mutual friends

        public void map(LongWritable key, Text val, Context context) throws IOException, InterruptedException {
            String[] mydata = val.toString().split("\\t");
            if (mydata.length <= 1) return;

            String friend_pair = mydata[0];
            String common_friends = mydata[1];
            String joined = friend_pair + ":" + common_friends;
            String[] common_friends_array = common_friends.split(",");

            pair_friends.set(joined);
            key1.set(common_friends_array.length);
            context.write(key1, pair_friends);
        }
    }

    public static class Reduce2 extends Reducer<IntWritable, Text, Text, Text> {
        private Text output = new Text();
        private Text value = new Text();

        public void reduce(IntWritable key, Iterable<Text> val, Context context) throws IOException, InterruptedException {
            List<String> pair_friends = new ArrayList<>();
            for (Text valu : val) {
                pair_friends.add(valu.toString());
            }

            String Ist = pair_friends.get(0);
            String[] Ist_pair = Ist.split(":");
            String pair = Ist_pair[0];
            String comm_friends = Ist_pair[1];

            String result = pair + "\t" + key.toString() + "\t" + comm_friends;
            output.set(result);

            context.write(output, value);
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
        if (otherargs.length != 4) {
            System.err.println(otherargs[0]);
            System.err.println("Incorrect arguments passed!");
            System.exit(2);
        }

        Job job1 = new Job(conf1, "Topten_firstMR");            // first mapreduce job
        job1.setJarByClass(Topten_commFriends.class);
        job1.setMapperClass(Map1.class);
        job1.setReducerClass(Reduce1.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(otherargs[1]));
        FileOutputFormat.setOutputPath(job1, new Path(otherargs[2]));
        boolean result1 = job1.waitForCompletion(true);

        if (result1) {
            Configuration conf2 = new Configuration();
            Job job2 = new Job(conf2, "Topten_secondMR");       // second mapreduce job
            job2.setJarByClass(Topten_commFriends.class);
            job2.setMapperClass(Map2.class);
            job2.setSortComparatorClass(Comparator_decreasing.class);
            job2.setReducerClass(Reduce2.class);

            job2.setMapOutputKeyClass(IntWritable.class);
            job2.setMapOutputValueClass(Text.class);

            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job2, new Path(otherargs[2]));
            FileOutputFormat.setOutputPath(job2, new Path(otherargs[3]));

            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }
    }
}
