import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Find_person{
    public static class Find_map extends Mapper<LongWritable, Text, Text, IntWritable> {

        public void map(LongWritable ikey, Text ivalue, Context context)
                throws IOException, InterruptedException {
            String[] lines=ivalue.toString().split(" ");
            Text key=new Text();
            IntWritable value=new IntWritable(1);
            for(int i=0;i<lines.length;i++){
                for(int j=0;j<lines.length;j++){
                    if(i!=j) {
                        key.set(lines[i] + "-" + lines[j]);
                        context.write(key, value);
                    }
                }
            }
        }
    }

    public static class Find_reduce extends Reducer<Text,IntWritable,Text,IntWritable>{
        public void reduce(Text ikey,Iterable<IntWritable> iterater,Context context)
                throws IOException, InterruptedException {
            int num=0;
            for(IntWritable i:iterater){
                num+=Integer.parseInt(i.toString());
            }
            context.write(ikey,new IntWritable(num));
        }
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir","F:\\hadoop-2.7.6");
        Configuration conf = new Configuration();
        Job job = new Job(conf);
        job.setJarByClass(Find_person.class);
        job.setJobName("find_person");
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(Find_map.class);
        job.setReducerClass(Find_reduce.class);
        //job.setInputFormatClass(TextInputFormat.class);
        //job.setOutputFormatClass(TextOutputFormat.class);
        Path path=new Path("hdfs://192.168.88.128:9000/relation");
        FileSystem fileSystem = path.getFileSystem(conf);// 根据path找到这个文件
        if (fileSystem.exists(path)) {
            fileSystem.delete(path, true);// true的意思是，就算output有东西，也一带删除
        }
        FileInputFormat.addInputPath(job, new Path("hdfs://192.168.88.128:9000/output/part-r-00000"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.88.128:9000/relation"));
        job.waitForCompletion(true);
    }

}

