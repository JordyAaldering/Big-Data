# CommonCrawl

In this final blog post we will bring everything we have learned so far together in order to do some actual big data analysis on a sample of the CommonCrawl.
We are going to do this by working on a real cluster, consisting of 2 main nodes and 11 workers, which we will connect to through a VPN.

We will use a relatively small part of one of the monthly crawls. Using hdfs in combination with the `du` command we can find the size of this data, along with the actual size on the disks including replicas.

```
[hadoop rubigdata]$ hdfs dfs -du -h -s /single-warc-segment
727.4 G  1.1 T  /single-warc-segment
```

## Linked Domains

We are going to do some analysis of this CommonCrawl, using standalone Spark code which we will submit to the cluster.
My plan is to find out how many times certain domains are linked. For instance, we would expect domains like 'facebook<area>.com' to be linked many times. It might be interesting to see how this compares to other domains.
We are also going to try to see how the web evolved by looking at how many times these domains were linked each year, which should show us if domains became more or less popular over time.

## Counting Links

The first step is to load all WARC records from the given location. We define a function `loadWarcRecords` which loads all WARCs, filters all valid ones, and then extracts and returns the records of these WARCs.

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

We start with some simple filtering steps to make sure the WARCs are valid and contain all the information we will need. Next we can map them to a triplet of (domain, year, body). Then, using `Jsoup`, we extract and parse all links from the body. Finally we do some filtering on these links, and return the results.

```scala
def getAllDomains(warcs: RDD[WarcRecord]) : RDD[(String, Int, String)] = {
    val cleanedWarcs = warcs
        // remove invalid warcs
        .filter(_ != null)
        // remove warcs of the incorrect type
        .filter(_.hasContentHeaders())
        .filter(_.getHeader().getHeaderValue("WARC-Type") == "response")
        .filter(_.getHttpHeaders().get("Content-Type") != null)
        .filter(_.isHttp())
    
    // split record into (domain, year, body)
    val domainYearBody = cleanedWarcs
        .filter(r => recordToDomain(r) != "")
        .map(r => (recordToDomain(r), tryGetYear(r), r.getHttpStringBody()))

    // generate (domain, year, link) triplets
    val linkedDomains = domainYearBody
        .flatMap { case (domain, year, body) => {
            val html = Jsoup.parse(body)
            // extract linked domains from all hrefs
            val links = html.select("a[href]").asScala
                .map(_.attr("href"))
                .map(urlToDomain)
            // filter empty links and ones that are equal to the domain
            links.filter(l => l != "" && l != domain)
                .map(l => (domain, year, l))
        }}
    
    return linkedDomains
}
```

Using these two functions we the get a list of all linked domains, along with their parent domain and year, which we can then save to a parquet file at the given location on the cluster that we can use later on.

```scala
// compute linked domain counts
val warcs = loadWarcRecords(infile, sc)
val domains = getAllDomains(warcs)

// save the RDD to a parquet file
domains.toDF("domain", "year", "link")
    .write.partitionBy("year")
    .mode(SaveMode.Overwrite)
    .parquet(outfile)
```

### Finding the year

Getting the year a specific link was first added is not actually possible, so instead we will look at when the entire page was last edited. Sadly I expect that this will not work very well, and that we will see that the year will almost exclusively be unknown or 2021.
We will come back to this later when we do the final analysis.
Future research could instead look at multiple crawls of different years to do some proper analysis on this evolution.

```scala
def tryGetYear(record: WarcRecord) : Int = {
    val headers = record.getHttpHeaders()
    var date = headers.get("Last-Modified")
    if (date == null) { date = headers.get("Date") }
    if (date == null) { return 0 }

    val getYear = "(\\d{4})".r
    val year = getYear.findFirstIn(date).getOrElse("0")
    return year.toInt
}
```

### Building and Running

We are now ready to build and run this code. To build it we simply use the command `sbt assembly`, which makes a fat jar of our compiled code, which also contains any external dependencies that our code requires.

Before running this code on the entire WARC segment, we will test it on a single file in this segment. To find a file we use `hdfs dfs -ls /single-warc-segment` to list all WARC files in this folder. After selecting any file from this list, we can submit our jar to the cluster, along with the required arguments.

```
spark-submit \
    --deploy-mode cluster \
    --num-executors 10 \
    --queue silver \
    target/scala-2.12/LinkedDomainsCounter-assembly-1.0.jar \
    /single-warc-segment/CC-MAIN-[].warc.gz \
    /user/JordyAaldering/out
```

This will only take a few minutes, after which we will have a bunch of parquet files in the `out` folder of the cluster. We are now ready to move on to the map-reduce part.

## Map-Reduce

We will do the map-reduce in a separate step. This is to avoid memory overhead problems, and so that we do not have to run the entire program again if we make a mistake in the map-reduce part, saving us a lot of time. As an added bonus, it also makes the code easier to read and understand.

The map-reduce implementation is actually very simple. We start by defining a function `reduceDomains`. This function takes an RDD of only a string and an integer, this is because for the map-reduce we omit the parent domain and only look at the link and year.
We filter the results a bit depending on the `minLinks` value. This filter  removes domains that were barely linked to, since these are not going to be interesting to us in the final analysis.

```scala
def reduceDomains(domains: RDD[(String, Int)], minLinks: Int) : RDD[(String, Int, Int)] = {
    val mapped = domains.map(_ -> 1)        // shape: ((domain, year), 1)
    val reduced = mapped.reduceByKey(_ + _) // shape: ((domain, year), amount)
        .filter(_._2 >= minLinks)
        .map(t => (t._1._1, t._1._2, t._2)) // shape: (domain, year, amount)
    return reduced
}
```

Now we can read the parquet file we created in the previous step, apply this map-reduce function on it, and save the results to a new parquet file. This new parquet file should be a lot smaller than the first one.

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

### Building and Running

After editing our `build.sbt` file to compile this new class and calling `sbt assembly`, we are ready to submit this code.

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

This also seems to be working fine, so we are now ready to apply our code to a larger dataset. We will go back and run our first program again, but now on the entire WARC segment. As we have seen at the start of this blog; this is a lot of data, so we will now use the gold queue along with an increased number of executors.

```
spark-submit \
    --deploy-mode cluster \
    --num-executors 20 \
    --queue gold \
    target/scala-2.12/LinkedDomainsCounter-assembly-1.0.jar \
    /single-warc-segment \
    /user/JordyAaldering/out
```

Though now we start running into memory errors. The solution is luckily not too hard, we just have to play with the Spark configuration settings a bit. I won't bore you with the details of these settings; if you are interested you can read up on what they do in the [documentation](https://spark.apache.org/docs/latest/configuration.html). After playing around with the settings for a while, this is what I ended up with.

```scala
val sparkConf = new SparkConf()
        .set("spark.memory.storageFraction", "0.3")
        .set("spark.memory.offHeap.enabled", "true")
        .set("spark.memory.offHeap.size", "10G")
        .set("spark.yarn.driver.memory", "5G")
        .set("spark.yarn.executor.memory", "5G")
        .set("spark.executor.memoryOverhead", "1G")
        .set("spark.shuffle.memoryFraction", "0")
        .set("spark.shuffle.io.maxRetries", "5")
        .set("spark.shuffle.io.retryWait", "10s")
        .set("spark.sql.shuffle.partitions", "200")
        .set("rdd.compression", "true")
        ...
```

Now our program will run properly. But this is going to take a while... We can use the Spark history server in a web UI to see how the process is doing. If we look deeper into the job we can also find out some useful summary metrics.

![Linked domains counter job](https://raw.githubusercontent.com/JordyAaldering/Big-Data/master/Assignment06/images/linked-domains-counter-job.png)

![Linked domains counter summary metrics](https://raw.githubusercontent.com/JordyAaldering/Big-Data/master/Assignment06/images/linked-domains-counter-metrics.png)

After about an hour and a half the job was completed. We can find out how much data we generated using the `du` command again.

```
[hadoop rubigdata]$ hdfs dfs -du -h -s /user/JordyAaldering/out
2.6 G  7.7 G  /user/JordyAaldering/out
```

We can now move on to the reducer. We will again use the gold queue, and we will filter any domains that appear less than a hundred times.
The reducer is a lot faster, and finishes within a few minutes. The output size is now also a lot smaller.

```
spark-submit \
    --deploy-mode cluster \
    --num-executors 20 \
    --queue gold \
    target/scala-2.12/ReduceDomains-assembly-1.0.jar \
    /user/JordyAaldering/out \
    /user/JordyAaldering/reduced100 \
    100
```

```
[hadoop rubigdata]$ hdfs dfs -du -h -s /user/JordyAaldering/reduced100
16.3 M  48.9 M  /user/JordyAaldering/reduced100
```

## Final Analysis

Now that the file size is a lot smaller, we can do the analysis on our own machine, using Zeppelin. This allows us to more easily try different things and, most importantly, make nice plots.

First we to copy the parquet files from the cluster to our local machine using `hdfs dfs -copyToLocal redbad:/user/JordyAaldering/reduced100 reduced100`. After which we can load in this parquet file.

```scala
val data = spark.read.parquet("reduced100")
data.createOrReplaceTempView("data")
```

Let's go back to something we have ignored for a while; the years. We will use Spark SQL to see if we were indeed correct in our fear that these years would not be very helpful.

```sql
SELECT year, count(year) FROM data
    GROUP BY year
    ORDER BY year DESC
```

![Linked domains years](https://raw.githubusercontent.com/JordyAaldering/Big-Data/master/Assignment06/images/linked-domains-years.png)

Sadly it is indeed the case that the year is almost exclusively 2021. Funnily enough the years 2022 and 2027 also occur a few times. From now on we will have to ignore the years, as they are not helpful.

Before moving on to the analysis we need to do a bit more preprocessing; it might be useful to us to work with only the top level domains. For instance, we might want to work with all domains ending in 'ru<area>.nl', instead of looking at 'portal<area>.ru<area>.nl' and 'sis<area>.ru<area>.nl' separately. We can easily do this with a regex, here we have to keep in mind that some domains end in, for instance, 'co<area>.uk'. After this we make sure to also filter out any invalid domains. This regex is not perfect, but it will do fine for our purposes.

```scala
val topLevel = data.withColumn("topLevel",
        regexp_extract(col("domain"), "\\w{4,}\\.\\w+[\\.\\w+]?$", 0))
    .filter("topLevel != ''")

topLevel.createOrReplaceTempView("data")
```

### Top domains

Yes! We are now ready to do some analysis on our data and see some results. Let's start by finding the 20 most popular domains.

```sql
SELECT topLevel, sum(amount) FROM data
    GROUP BY topLevel, year
    ORDER BY sum(amount) DESC
    LIMIT 20
```

![Linked domains top 20](https://raw.githubusercontent.com/JordyAaldering/Big-Data/master/Assignment06/images/linked-domains-top-20.png)

And in a surprise to no-one, Facebook is linked the most with a total of 28.4 million links!

Now that we have both the domain itself and its top level domain, we can also find out how many subdomains each top level domain has.

```sql
SELECT topLevel, count(domain) FROM data
    GROUP BY topLevel
    ORDER BY count(domain) DESC
    LIMIT 20
```

![Linked domains num subdomains](https://raw.githubusercontent.com/JordyAaldering/Big-Data/master/Assignment06/images/linked-domains-num-subdomains.png)

Here we see that 'blogspot<area>.com' has a whopping 22,324 subdomains! Which probably makes sense; I'm guessing that every person that has a blog gets their own subdomain, like with GitHub pages. Actually, let's validate that theory.

```sql
SELECT topLevel, count(domain) FROM data
    WHERE topLevel = 'github.io'
    GROUP BY topLevel
```

This shows us that 'github<area>.io' has a total of 97 subdomains, so it seems our theory makes sense.

## Conclusion

This concludes my analysis of the CommonCrawl. There is still a lot of room for improvement and future research, but I am very happy with the achieved results. Thank you for reading! :)

<p align="center">
<img src="https://media.giphy.com/media/UVw2vDdLSepnUSpKHx/giphy.gif" width="500" height="500"/>
</p>

---

The code shown in this blog can be found on GitHub [here](https://github.com/JordyAaldering/Big-Data/tree/master/Assignment06).
