package org.rubigdata

import org.apache.hadoop.io.NullWritable
import de.l3s.concatgz.io.warc.{WarcGzInputFormat,WarcWritable}
import de.l3s.concatgz.data.WarcRecord

import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.SparkSession
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
            .set("spark.memory.offHeap.size", "512m")
            .set("spark.yarn.driver.memory", "16g")
            .set("spark.yarn.executor.memory", "16g")
            .set("spark.yarn.executor.memoryOverhead", "16g")
            .set("rdd.compression", "true")
            .registerKryoClasses(Array(classOf[WarcRecord]))

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
    def loadWarcs(infile: String, sc: SparkContext) : RDD[(NullWritable, WarcWritable)] = {
        val warcs = sc.newAPIHadoopFile(
                infile,
                classOf[WarcGzInputFormat],
                classOf[NullWritable],  // Key
                classOf[WarcWritable]   // Value
            )
            .cache()
        
        println(s"> loaded ${warcs.count()} warc files from `${infile}'")
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

    /** Gets all domains that were linked to by web pages
     *  
     *  @param warcs the warc files to operate on
     *  @return a list of all linked domains
     */
    def getAllDomains(warcs: RDD[(NullWritable, WarcWritable)]) : RDD[String] = {
        val domains = warcs
            // get all http records and their corresponding domains
            .map(_._2.getRecord())
            .map(r => (recordToDomain(r), r))
            .filter(dr => dr._1 != "" && dr._2.isHttp())
            // get all linked domains that are not equal to the domain itself
            .flatMap { case (domain, record) => {
                // get the parsed http body of the record
                val body = Jsoup.parse(record.getHttpStringBody())
                // extract all linked domains from the body
                val hrefs = body.select("a[href]").asScala.map(_.attr("href"))
                val links = hrefs.map(urlToDomain)
                // filter linked domains
                links.filter(l => l != "" && l != domain)
            }}
            .cache()
        
        return domains
    }

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

        // compute linked domain counts
        val warcs = loadWarcs(infile, sc)
        val domains = getAllDomains(warcs)
        val counts = reduceDomains(domains)

        // print a few domains
        printDomains(counts, 20)
        // save the entire RDD to a file
        counts.saveAsTextFile(outfile)

        // finalize session
        sparkSession.stop()
    }
}
