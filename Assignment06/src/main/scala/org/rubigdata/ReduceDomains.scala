package org.rubigdata

import org.apache.spark.{SparkConf}
import org.apache.spark.sql.{SparkSession,SaveMode}
import org.apache.spark.rdd.RDD

import collection.JavaConverters._

object ReduceDomains {
    /** Initializes and sets up a new spark session
     *  
     *  @return a new spark session
     */
    def setupSparkSession() : SparkSession = {
        val sparkConf = new SparkConf()
            .setAppName("ReduceDomains")
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .set("spark.memory.storageFraction", "0.3")
            .set("spark.memory.offHeap.enabled", "true")
            .set("spark.memory.offHeap.size", "10G")
            .set("spark.yarn.driver.memory", "5G")
            .set("spark.yarn.executor.memory", "5G")
            .set("spark.executor.memoryOverhead", "1G")
            .set("rdd.compression", "true")
            .set("spark.shuffle.memoryFraction", "0")
            .set("spark.shuffle.io.maxRetries", "5")
            .set("spark.shuffle.io.retryWait", "10s")
            .set("spark.sql.shuffle.partitions", "200")
            .registerKryoClasses(Array(classOf[Integer]))
            .registerKryoClasses(Array(classOf[String]))

        val sparkSession = SparkSession.builder()
            .config(sparkConf)
            .getOrCreate()
        
        println(s"> running spark version ${sparkSession.version}")
        return sparkSession
    }

    /** Uses map-reduce to count how many times each domain was linked
     *  It also separated domains by the year they were linked in
     *  
     *  @param domains a list of all domains that were linked
     *  @param minLinks the minimum amount of times a domain has to be linked
     *  @return a mapping of domains to how many times were linked
     */
    def reduceDomains(domains: RDD[(String, Int)], minLinks: Int) : RDD[(String, Int, Int)] = {
        val mapped = domains.map(_ -> 1)        // shape: ((domain, year), 1)
        val reduced = mapped.reduceByKey(_ + _) // shape: ((domain, year), amount)
            .filter(_._2 >= minLinks)
            .map(t => (t._1._1, t._1._2, t._2)) // shape: (domain, year, amount)
        return reduced
    }

    def main(args: Array[String]) {
        // check arguments
        if (args.length != 2) {
            throw new RuntimeException("usage: ReduceDomains <infile> <outfile> <min links>")
        }

        // parse arguments
        val infile = args(0)
        val outfile = args(1)
        val minLinks = args(2).toInt

        // start spark session
        val sparkSession = setupSparkSession()
        import sparkSession.implicits._

        // read the parquet and get only the linked domain and the year
        val domainsRows = sparkSession.read.parquet(infile).rdd
        val domains = domainsRows.map(r => (r.getString(1), r.getInt(2)))
        val reduced = reduceDomains(domains, minLinks)

        // save the RDD to a parquet file
        reduced.toDF("domain", "year", "amount")
            .write.partitionBy("year")
            .mode(SaveMode.Overwrite)
            .parquet(outfile)

        // finalize session
        sparkSession.stop()
    }
}
