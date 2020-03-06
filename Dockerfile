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
RUN mv /jdk1.8.0_241 $JAVA_HOME
RUN mv /scala-2.13.1 $SCALA_HOME

CMD ["/bin/bash"]