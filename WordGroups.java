package mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordGroups {
    public static class GroupMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().toLowerCase();
            String[] words = line.split("[^a-z]+");

            Set<String> stopWords = new HashSet<>(Arrays.asList( //hashset used cause of O(1)
                "the", "a", "an", "and", "of", "to", "in", "am", "is", "are", "at", "not"
            ));

            for (String word : words) {
                if (word.isEmpty() || stopWords.contains(word)) continue;

                char[] chars = word.toCharArray(); //so basically it converst "hello" into ['h', 'e', 'l', 'l', 'o'] 
                Arrays.sort(chars); // so that we can sort it alphabetically for common key
                String sorted = new String(chars);

                context.write(new Text(sorted), new Text(word));
            }
        }
    }

    public static class GroupReducer extends Reducer<Text, Text, NullWritable, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, Integer> wordCounts = new HashMap<>();
            int totalCount = 0;

            //frequencies
            for (Text val : values) {
                String word = val.toString();
                wordCounts.put(word, wordCounts.getOrDefault(word, 0) + 1);
                totalCount++;
            }

            // sorted words by lexicograpical
            List<String> uniqueWords = new ArrayList<>(wordCounts.keySet());
            Collections.sort(uniqueWords);

            int groupSize = uniqueWords.size();
            String joinedWords = String.join(" ", uniqueWords);
            String output = groupSize + "\t" + totalCount + "\t" + joinedWords;

            context.write(NullWritable.get(), new Text(output));
        }
    }
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: WordGroups <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();

        Job groupJob = Job.getInstance(conf, "Group Word Anagrams");
        groupJob.setJarByClass(WordGroups.class);
        groupJob.setMapperClass(GroupMapper.class);
        groupJob.setReducerClass(GroupReducer.class);
        groupJob.setMapOutputKeyClass(Text.class);
        groupJob.setMapOutputValueClass(Text.class);
        groupJob.setOutputKeyClass(NullWritable.class);
        groupJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(groupJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(groupJob, new Path(args[1]));


        System.exit(groupJob.waitForCompletion(true) ? 0 : 1);
    }
}

/*
 * mvn archetype:generate \
  -DgroupId=mapreduce \
  -DartifactId=wordgroups \
  -DarchetypeArtifactId=maven-archetype-quickstart \
  -DinteractiveMode=false


  cd wordgroups
    rm -f src/main/java/mapreduce/*
    rm -f src/test/java/mapreduce/*


    <properties>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <maven.compiler.source>1.8</maven.compiler.source>
        <maven.compiler.target>1.8</maven.compiler.target>
    </properties>

    <dependencies>
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-client</artifactId>
            <version>2.7.3</version>
        </dependency>
    </dependencies>

    nano src/main/java/mapreduce/WordGroups.java



    rm -rf build/*
    mkdir -p build
    cd src/main/java/mapreduce/
    hadoop com.sun.tools.javac.Main WordGroups.java -d build
    jar -cvf WordGroups.jar -C build/ .
    cd ~/midterm/wordgroups
    mvn clean package
    hadoop jar ./target/wordgroups-1.0-SNAPSHOT.jar mapreduce.WordGroups /user/adampap/Frankenstein /user/bigdata48/midterm_output
    hdfs dfs -cat /user/bigdata48/midterm_output/part*
    hdfs dfs -rm -r /user/bigdata48/midterm_output
    
    <project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
		  xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/maven-v4_0_0.xsd">
		  <modelVersion>4.0.0</modelVersion>
		  <groupId>mapreduce</groupId>
		  <artifactId>wordgroups</artifactId>
		  <packaging>jar</packaging>
		  <version>1.0-SNAPSHOT</version>
		  <name>wordgroups</name>
		  <url>http://maven.apache.org</url>
		  <properties>
		    <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
		    <maven.compiler.source>1.8</maven.compiler.source>
		    <maven.compiler.target>1.8</maven.compiler.target>
		  </properties>
		
		  <dependencies>
		    <dependency>
		      <groupId>junit</groupId>
		      <artifactId>junit</artifactId>
		      <version>3.8.1</version>
		      <scope>test</scope>
		    </dependency>
		    <dependency>
		      <groupId>org.apache.hadoop</groupId>
		      <artifactId>hadoop-client</artifactId>
		      <version>2.7.3</version>
		  </dependency>
		  </dependencies>
		</project>
		
 */