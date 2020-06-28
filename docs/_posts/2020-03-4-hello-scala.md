---
layout: post
title: "Hello Scala"
date: 2020-03-04
---

After downloading and installing Docker and Scala I created a new project. I first had to download and add the [Java JDK](https://www.oracle.com/java/technologies/javase-jdk8-downloads.html) and [Scala](https://www.scala-lang.org/download/) files to the project folder. Next I needed a Dockerfile:

```dockerfile
# Define the environment
FROM ubuntu

# Define environment variables
ENV SHARE /usr/local/share
ENV SCALA_HOME $SHARE/scala
ENV JAVA_HOME $SHARE/java
ENV PATH=$SCALA_HOME/bin:$JAVA_HOME/bin:$PATH

# Move over JDK and Scala
ADD jdk-8u241-linux-i586.tar.gz /
ADD scala-2.13.1.tgz /

# Get JDK and Scala into place
# RUN ls
RUN mv /jdk1.8.0_241 $JAVA_HOME
RUN mv /scala-2.13.1 $SCALA_HOME

CMD ["/bin/bash"]
```

At this point Docker knew how the image had to be set up. My docker id is _jordyaaldering_ so I used that for the tag, including the file name _docker-scala_. I had to temporarily add ```RUN ls``` to find the JDK and Scala file names. This line could later be removed.  I was now ready to build the image.

```bash
docker build -t jordyaaldering/docker-scala .
```

After creating the image I pushed it to my Docker Hub.

```bash
docker push jordyaaldering/docker-scala
```

Now I could use this image with an interative Scala shell. So that I could try out Scala.

```bash
docker run -it jordyaaldering/docker-scala
```

After trying out Scala for a bit I was ready to create a sample _Hello World_ function and start a Docker container with a mounted volume to be able to compile and run the code.

```scala
object HelloWorld extends App {
    println("Hello, world")
}
```

```bash
docker run -it -v /src:/src jordyaaldering/docker-scala
```

Finally I was ready to compile and run the code.

```bash
cd /scala_example
scalac helloWorld.scala
scala main
```

The source files of this assignment can be found on my [GitHub](https://github.com/JordyAaldering/Big-Data/tree/master/Assignment01).
