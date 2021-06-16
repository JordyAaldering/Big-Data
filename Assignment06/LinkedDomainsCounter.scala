package org.rubigdata

import org.apache.hadoop.io.NullWritable
import de.l3s.concatgz.io.warc.{WarcGzInputFormat,WarcWritable}
import de.l3s.concatgz.data.WarcRecord

import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.sql.{SparkSession,SaveMode}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD

import org.jsoup.Jsoup
import org.jsoup.nodes.Document

import collection.JavaConverters._
import collection.mutable.{Buffer,ArrayBuffer}
import java.net.URI
import util.Try

object LinkedDomainsCounter {
    /** @return a new spark session */
    def setupSparkSession() : SparkSession = {
        val sparkConf = new SparkConf()
            .setAppName("LinkedDomainsCounter")
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .set("spark.memory.storageFraction", "0.6")
            .set("spark.memory.offHeap.enabled", "true")
            .set("spark.memory.offHeap.size", "10G")
            .set("spark.yarn.driver.memory", "1G")
            .set("spark.yarn.executor.memory", "1G")
            .set("spark.executor.memoryOverhead", "1G")
            .set("rdd.compression", "true")
            .set("spark.shuffle.io.maxRetries", "5")
            .set("spark.shuffle.io.retryWait", "10s")
            .registerKryoClasses(Array(classOf[WarcRecord]))
            .registerKryoClasses(Array(classOf[String]))

        val sparkSession = SparkSession.builder()
            .config(sparkConf)
            .getOrCreate()
        
        println(s"> running spark version ${sparkSession.version}")
        return sparkSession
    }

    /** Loads warcs from a local file or folder
     *  
     *  @param infile the name of the warc file or folder
     *  @param sc the current spark context
     *  @return the loaded warcs as an RDD
     */
    def loadWarcRecords(infile: String, sc: SparkContext) : RDD[WarcRecord] = {
        val warcs = sc.newAPIHadoopFile(
                infile,
                classOf[WarcGzInputFormat],
                classOf[NullWritable],  // Key
                classOf[WarcWritable]   // Value
            )
            .filter(_._2.isValid())
            .map(_._2.getRecord())
            .repartition(50)
        
        println(s"> loaded warc files from `${infile}'")
        return warcs
    }

    /** @return the domain of a given url */
    def urlToDomain(url: String) : String = {
        val opt = Try { new URI(url).getHost() }.toOption
        return opt match {
            case None => ""
            case Some(null) => ""
            case Some(dom) => if (dom.startsWith("www.")) dom.substring(4) else dom
        }
    }

    /** @return the source domain of a given record */
    def recordToDomain(record: WarcRecord) : String = {
        // get the url from the header
        val url = record.getHeader().getUrl()
        if (url == null || url == "") { return "" }

        // extract the domain from the url
        val domain = urlToDomain(url)
        return domain
    }

    /** Tries to get the last modified date of a record
     *  
     *  @param record the record to process
     *  @return the last modified date of a record
     */
    def tryGetYear(record: WarcRecord) : String = {
        val headers = record.getHttpHeaders()
        var date = headers.get("Last-Modified")
        if (date == null) {
            date = headers.get("Date")
        }

        if (date != null) {
            return date.substring(12, 16)
        }

        date = record.getHeader().getDate()
        if (date != null) {
            return date.substring(0, 4)
        }

        return "N/A"
    }

    /** Gets all domains that were linked to by web pages
     *  
     *  @param warcs the warc files to operate on
     *  @return a list of all linked domains
     */
    def getAllDomains(warcs: RDD[WarcRecord]) : RDD[(String, String, String)] = {
        val domains = warcs
            // get all http records and corresponding domains
            .filter(_.isHttp())
            .filter(r => recordToDomain(r) != "")
            .map(r => (recordToDomain(r), tryGetYear(r), r.getHttpStringBody()))
            .repartition(400)
            // get all linked domains that are not equal to the domain itself
            .flatMap { case (domain, year, body) => {
                // get the parsed http body of the record
                val html = Jsoup.parse(body)
                // extract all linked domains from the body
                val links = html.select("a[href]").asScala
                    .map(_.attr("href"))
                    .map(urlToDomain)
                // filter linked domains
                links.filter(l => l != "" && l != domain)
                    .map(l => (domain, year, l))
            }}
        
        return domains
    }

    /*
    /** Uses map-reduce to count how many times each domain was linked
     *  
     *  @param domains a list of all domains that were linked
     *  @return a mapping of domains to how many times were linked
     */
    def reduceDomains(domains: RDD[String]) : RDD[(String, Int)] = {
        val mapped = domains.map(_ -> 1)
        val reduced = mapped.reduceByKey(_ + _).cache()
        return reduced
    }

    /** Prints the first few domains
     *  
     *  @param counts a mapping of domains to how many times were linked
     *  @param n the maximum number of domains to print
     */
    def printDomains(counts: RDD[(String, Int)], n: Int) {
        println(s"> showing first ${n} domains")
        counts.take(n)
            .foreach { case (domain, count) =>
                println(s"${domain} is linked ${count} times")
            }
    }
    */

    // spark-submit --deploy-mode cluster --queue default target/scala-2.12/LinkedDomainsCounter-assembly-1.0.jar /single-warc-segment /user/JordyAaldering/out
    // spark-submit --deploy-mode cluster --queue default target/scala-2.12/LinkedDomainsCounter-assembly-1.0.jar /single-warc-segment/CC-MAIN-20210410105831-20210410135831-00639.warc.gz /user/JordyAaldering/out
    // yarn logs -log_files stderr -applicationId application_1623272363921_0018
    def main(args: Array[String]) {
        // check arguments
        if (args.length != 2) {
            throw new RuntimeException("usage: LinkedDomainsCounter <infile> <outfile>")
        }

        // parse arguments
        val infile = args(0)
        val outfile = args(1)

        // start spark session
        val sparkSession = setupSparkSession()
        val sc = sparkSession.sparkContext

        import sparkSession.implicits._

        // compute linked domain counts
        val warcs = loadWarcRecords(infile, sc)
        val domains = getAllDomains(warcs)
        //val counts = reduceDomains(domains)

        domains.toDF("domain", "year", "link")
            .write.partitionBy("domain", "year")
            .mode(SaveMode.Overwrite)
            .parquet(outfile)

        // print a few domains
        //printDomains(counts, 20)
        // save the entire RDD to a file
        //counts.saveAsTextFile(outfile)

        // finalize session
        sparkSession.stop()
    }
}
