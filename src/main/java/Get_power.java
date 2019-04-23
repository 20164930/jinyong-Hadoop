import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Get_power{
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable ikey, Text ivalue, Context context)
                throws IOException, InterruptedException {
            if(ikey.toString().equals("0")){
                return;
            }else {
                String[] lines = ivalue.toString().split("\t");
                Text key = new Text();
                Text value = new Text();
                String[] names = lines[0].split("-");
                if (names[0].length() > 0) {
                    key.set(names[0]);
                    value.set(names[1] + ":" + lines[1]);
                    context.write(key, value);
                }
            }
        }
    }

    public static class Reduce extends Reducer<Text,Text,Text,Text> {
        public void reduce(Text ikey, Iterable<Text> iterater, Context context)
                throws IOException, InterruptedException {
            StringBuilder relation = new StringBuilder();
            ArrayList<String> list=new ArrayList<>();
            Text value = new Text();
            int n = 0;
            for (Text t : iterater) {
                list.add(t.toString());
                n += Integer.parseInt(t.toString().split(":")[1]);
            }
            for (int i=0;i<list.size();i++) {
                System.out.println(list.get(i));
                relation.append(list.get(i).split(":")[0] + ":" + ((double)(Integer.parseInt(list.get(i).split(":")[1])) / n) + ";");
            }
            value.set("0.1#"+relation.toString());
            context.write(ikey, value);
        }
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir","F:\\hadoop-2.7.6");
        Configuration conf = new Configuration();
        Job job = new Job(conf);
        job.setJarByClass(Get_power.class);
        job.setJobName("Get_power");
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        Path path=new Path("hdfs://192.168.88.128:9000/power");
        FileSystem fileSystem = path.getFileSystem(conf);// 根据path找到这个文件
        if (fileSystem.exists(path)) {
            fileSystem.delete(path, true);// true的意思是，就算output有东西，也一带删除
        }
        FileInputFormat.addInputPath(job, new Path("hdfs://192.168.88.128:9000/relation/part-r-00000"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.88.128:9000/power"));
        job.waitForCompletion(true);
    }
}


