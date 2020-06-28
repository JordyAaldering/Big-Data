---
layout: post
title: "Hello Hadoop"
date: 2020-03-26
---

Before starting with this assignment I took some time to get familiar with Docker and Scala, my experience can be found on my [previous blog post](https://jordyaaldering.github.io/2020/03/01/hello-scala).

### Hadoop Commands

| Command | Meaning |
| ------- | ------- |
| `sbin/start-dfs.sh` | Starts HDFS, including the nodes. |
| `bin/hdfs dfs -mkdir /user` `/root` | Creates new directories user and root within the file system. |
| `bin/hdfs dfs -put etc/hadoop input` | The hadoop directory is copied to the input folder in the file system. |
| `bin/hadoop jar ... grep input output 'dfs[a-z.]+'` | Runs MapReduce on input and writes everything that matches the Regex to output. |
| `bin/hdfs dfs -get output output` | The output from the file system is copied to a local Docker file. |
| `bin/hdfs dfs -ls hdfs://localhost:9001/user/root/input` | Lists all files in the input folder in the file system. |
| `sbin/stop-dfs.sh` | Stops the file system. |

### Mappers

These are the mappers I used for finding the number of characters, words, lines respectively.

```java
public static class CharMapper
        extends Mapper<Object, Text, Text, IntWritable> {
    private final Text character = new Text("Characters");

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        int length = line.length();
        context.write(character, new IntWritable(length));
    }
}

public static class WordMapper
        extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private final Text word = new Text("Words");

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            itr.nextToken();
            context.write(word, one);
        }
    }
}

public static class LineMapper
        extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private final Text line = new Text("Lines");

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        context.write(line, one);
    }
}
```

Running these functions gave the following results.

```bash
Characters  5545144
Words	    959301
Lines	    147838
```

### Romeo & Juliet

For finding the number of occurences of Juliet and Romeo I used an approach that looks similar to the word count. This approach only makes one pass over the corpus. Small differences like capital letters are also taken into account.

```java
public static class NameMapper
        extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private final Text Romeo = new Text("Romeo");
    private final Text Juliet = new Text("Juliet");

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            String name = itr.nextToken().replaceAll("\\W", "");
            if (name.equalsIgnoreCase("Romeo")){
                context.write(Romeo, one);
            } else if (name.equalsIgnoreCase("Juliet")){
                context.write(Juliet, one);
            }
        }
    }
}
```

This gave the following results, which tells us that Romeo appeared more often than Juliet.

```bash
Juliet	195
Romeo	291
```

The source files of this assignment can be found on my [GitHub](https://github.com/JordyAaldering/Big-Data/tree/master/Assignment02).
I also followed this [tutorial by Thijs Gelton](https://github.com/rubigdata/forum-2020/issues/5) to allow local development in Intellij.
