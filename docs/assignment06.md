# CommonCrawl

In this final blog post we will bring everything we have learned so far together in order to do real big data analysis on a sample of the CommonCrawl.

We will do this by working on a real cluster consisting of 2 main nodes and 11 workers, which we will connect to through a VPN.

To start we will use a relatively 'small' part of one of the monthly crawls. Using hdfs we can use the `du` command to show the size of the data along with the actual size on the disks, including replicas.

```
[hadoop rubigdata]$ hdfs dfs -du -h -s /single-warc-segment
727.4 G  1.1 T  /single-warc-segment
```

## Linked Domains

Our goal will be to do some analysis of this CommonCrawl, using standalone Spark code which we will submit to the cluster.
My plan is to find out how many times certain domains are linked. For instance, we would expect domains like 'facebook<area>.com' to be linked many times. It might be interesting to see how this compares to other domains.

## Counting Links

The first step is to load all WARC records. We define a function `loadWarcRecords` which loads all WARCs, filters all valid ones, and then extracts the records of these WARCs.

```scala
def loadWarcRecords(infile: String, sc: SparkContext) : RDD[WarcRecord] = {
    val warcs = sc.newAPIHadoopFile(
            infile,
            classOf[WarcGzInputFormat],
            classOf[NullWritable],  // Key
            classOf[WarcWritable]   // Value
        )
        .filter(_._2.isValid())
        .map(_._2.getRecord())
    
    println(s"> loaded warc files from `${infile}'")
    return warcs
}
```

Now we can iterate over all these WARC records, and find all the domains they link to. We are going to exclude the links that point to the parent domain itself. For this we define a function `getAllDomains`, which returns an RDD of links containing the parent domain, year, and link itself.

We start with some simple filtering steps to make sure the WARCs contain all the information we need. We then map then to a triplet of (domain, year, body). Then, using `Jsoup`, we extract all links from the body, after which we do some filtering.

```scala
def getAllDomains(warcs: RDD[WarcRecord]) : RDD[(String, Int, String)] = {
    val cleanedWarcs = warcs
        .filter(_ != null)
        .filter(_.hasContentHeaders())
        .filter(_.getHeader().getHeaderValue("WARC-Type") == "response")
        .filter(_.getHttpHeaders().get("Content-Type") != null)
        .filter(_.isHttp())
    
    val domainYearBody = cleanedWarcs
        .filter(r => recordToDomain(r) != "")
        .map(r => (recordToDomain(r), tryGetYear(r), r.getHttpStringBody()))

    val linkedDomains = domainYearBody
        .flatMap { case (domain, year, body) => {
            val html = Jsoup.parse(body)
            val links = html.select("a[href]").asScala
                .map(_.attr("href"))
                .map(urlToDomain)
            links.filter(l => l != "" && l != domain)
                .map(l => (domain, year, l))
        }}
    
    return linkedDomains
}
```

Using these two functions we can find the linked domains, which we can then save to a Parquet file.

```scala
val warcs = loadWarcRecords(infile, sc)
val domains = getAllDomains(warcs)

domains.toDF("domain", "year", "link")
    .write.partitionBy("domain")
    .mode(SaveMode.Overwrite)
    .parquet(outfile)
```

### Building and Running

We are now ready to build and run this code. To build it we simply use the command `sbt assembly`, which makes a fat jar of our compiled code, along with any external dependencies.

Before running this code on the entire WARC segment, we will test it on a single file in this segment. To find a file we call `hdfs dfs -ls /single-warc-segment` in order to list all WARC files in this folder. After selecting a file from this list, we can submit our jar to the cluster, along with the required arguments.

```
spark-submit \
    --deploy-mode cluster \
    --num-executors 10 \
    --queue silver \
    target/scala-2.12/LinkedDomainsCounter-assembly-1.0.jar \
    /single-warc-segment/CC-MAIN-[].warc.gz \
    /user/JordyAaldering/out
```

## Map-Reduce

We will do the map-reduce in a separate step. This is to avoid memory overhead problems. As a bonus it also makes the code easier to read and understand.

The map-reduce implementation is actually very simple. We define a function `reduceDomains` to do this.
As you can see this function takes an RDD of only a string and an integer. This is because for the map-reduce we omit the parent domain, and we only take the domain of the link and its year.

```scala
def reduceDomains(domains: RDD[(String, Int)], minLinks: Int) : RDD[(String, Int, Int)] = {
    val mapped = domains.map(_ -> 1)        // shape: ((domain, year), 1)
    val reduced = mapped.reduceByKey(_ + _) // shape: ((domain, year), amount)
        .filter(_._2 >= minLinks)
        .map(t => (t._1._1, t._1._2, t._2)) // shape: (domain, year, amount)
    return reduced
}
```

Now we can read the parquet file we just created, apply this map-reduce, and save the results to a new file; ready for analysis.

```scala
// read the parquet and convert it to an RDD
val domainsRows = sparkSession.read.parquet(infile).rdd
// get only the linked domain and the year
val domains = domainsRows.map(r => (r.getString(1), r.getInt(0)))
val reduced = reduceDomains(domains)

// save the reduced RDD to a parquet file
reduced.toDF("domain", "year", "amount")
    .write.partitionBy("domain")
    .mode(SaveMode.Overwrite)
    .parquet(outfile)
```

After editing our `build.sbt` file to compile this new class and calling `sbt assembly`, we are ready to submit this code. It works similarly to before.

```
spark-submit \
    --deploy-mode cluster \
    --num-executors 10 \
    --queue silver \
    target/scala-2.12/ReduceDomains-assembly-1.0.jar \
    /user/JordyAaldering/out \
    /user/JordyAaldering/reduced \
    50
```

# Analysis

This al seems to be working fine, so we are now ready to apply our code to the entire WARC segment. This is a lot of data, so we will use the gold queue, along with an increased number of executors.

```
spark-submit \
    --deploy-mode cluster \
    --num-executors 20 \
    --queue gold \
    target/scala-2.12/LinkedDomainsCounter-assembly-1.0.jar \
    /single-warc-segment \
    /user/JordyAaldering/out
```

```
spark-submit \
    --deploy-mode cluster \
    --num-executors 20 \
    --queue gold \
    target/scala-2.12/ReduceDomains-assembly-1.0.jar \
    /user/JordyAaldering/out \
    /user/JordyAaldering/reduced
    100
```

This is going to take a while... We can use the Spark history server in a web UI to see how the process is doing.

![Linked domains counter job](https://raw.githubusercontent.com/JordyAaldering/Big-Data/master/Assignment06/images/linked-domains-counter-job.png)

If we look into this job we can also find out some summary metrics.

![Linked domains counter summary metrics](https://raw.githubusercontent.com/JordyAaldering/Big-Data/master/Assignment06/images/linked-domains-counter-metrics.png)

---

The code shown in this blog can be found on GitHub [here](https://github.com/JordyAaldering/Big-Data/tree/master/Assignment06).
