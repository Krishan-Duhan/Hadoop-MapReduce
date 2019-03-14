import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

public class InMemoryJoin {
    static String usrA;
    static String usrB;

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        // Hashmap stores <userid, name&city of friends of userid>
        HashMap<String, String> map1 = new HashMap<>();

        private Text usr = new Text();
        private Text friend_list = new Text();

        @Override
        protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            Configuration confg = new Configuration();
            FileSystem fs = FileSystem.get(confg);
            URI[] loc = context.getCacheFiles();
            Path path = new Path(loc[0].getPath());
            read_file(fs.open(path));
        }

        void read_file(FSDataInputStream path) throws IOException, InterruptedException {
            BufferedReader buff_rdr = new BufferedReader(new InputStreamReader(path));
            String line = buff_rdr.readLine();
            while (line != null) {
                String[] usr_data = line.split(",");
                if (usr_data.length == 10) {
                    String value = usr_data[1] + ":" + usr_data[4];
                    map1.put(usr_data[0].trim(), value);
                }
                line = buff_rdr.readLine();
            }
        }

        // Suppose soc_data.txt has <0   1,2> as an entry
        // map will emit <0,1> <name&city of all friends of 0> as <key> <value>
        //               <0,2> <name&city of all friends of 0>
        public void map(LongWritable key, Text val, Context context) throws IOException, InterruptedException {
            String[] mydata = val.toString().split("\\t");
            if (mydata.length <= 1) return;
            String u_id = mydata[0];
            if ((u_id.equals(usrA)) || (u_id.equals(usrB))) {
                String[] friends = mydata[1].split(",");
                String emit_key;
                StringBuilder emit_val = new StringBuilder();
                for (String friend : friends) {
                    if (map1.containsKey(friend)) {
                        emit_val.append(map1.get(friend));
                        emit_val.append(",");
                    }
                }
                emit_val.deleteCharAt(emit_val.length() - 1);           //delete the last comma
                //emit_val.append("]");

                for (String friend : friends) {
                    if (Integer.parseInt(u_id) > Integer.parseInt(friend)) {
                        emit_key = friend + "," + u_id;
                    } else {
                        emit_key = u_id + "," + friend;
                    }
                    usr.set(emit_key);
                    friend_list.set(emit_val.toString());
                    context.write(usr, friend_list);
                }
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        private Text common_frnds = new Text();

        public void reduce(Text key, Iterable<Text> val, Context context) throws IOException, InterruptedException {
            String[] k = key.toString().split(",");
            if ((k[0].equals(usrA) && k[1].equals(usrB)) || (k[0].equals(usrB) && k[1].equals(usrA))) {
                String[] lists = new String[2];
                int i = 0;

                // Store the lists of friends for <usrA, usrB> key pair
                for (Text list : val) {
                    lists[i++] = list.toString();
                }

                String[] list1 = lists[0].split(",");
                String[] list2 = lists[1].split(",");
                Set<String> set1 = new HashSet<>(Arrays.asList(list1));
                System.out.println("set1 is: " + set1);
                Set<String> set2 = new HashSet<>(Arrays.asList(list2));
                System.out.println("set2 is: " + set2);
                set1.retainAll(set2);
                List<String> res = new ArrayList<>(set1);
                System.out.println("common frinds in string: " + res);

                common_frnds.set(String.join(",", res));
                context.write(key, common_frnds);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherargs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherargs.length != 6) {
            System.err.println(otherargs[0]);
            System.err.println("Incorrect arguments passed!");
            System.exit(2);
        }

        usrA = otherargs[1];
        usrB = otherargs[2];
        Job job = new Job(conf, "commonfriends");
        job.addCacheFile(new Path(otherargs[4]).toUri());

        job.setJarByClass(InMemoryJoin.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherargs[3]));
        FileOutputFormat.setOutputPath(job, new Path(otherargs[5]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
