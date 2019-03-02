import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.*;

public class CommonFriends {

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
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

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
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

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherargs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherargs.length != 3) {
            System.err.println(otherargs[0]);
            System.err.println("Incorrect arguments passed!");
            System.exit(2);
        }

        Job job = new Job(conf, "commonfriends");
        job.setJarByClass(CommonFriends.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherargs[1]));
        FileOutputFormat.setOutputPath(job, new Path(otherargs[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
