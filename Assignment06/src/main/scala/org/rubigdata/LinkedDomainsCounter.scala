package org.rubigdata

import org.apache.hadoop.io.NullWritable
import de.l3s.concatgz.io.warc.{WarcGzInputFormat,WarcWritable}
import de.l3s.concatgz.data.WarcRecord

import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.sql.{SparkSession,SaveMode}
import org.apache.spark.rdd.RDD

import org.jsoup.Jsoup

import collection.JavaConverters._
import java.net.URI
import util.Try

object LinkedDomainsCounter {
    /** Initializes and sets up a new spark session
     *  
     *  @return a new spark session
     */
    def setupSparkSession() : SparkSession = {
        val sparkConf = new SparkConf()
            .setAppName("LinkedDomainsCounter")
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
            .registerKryoClasses(Array(classOf[WarcRecord]))
            .registerKryoClasses(Array(classOf[Integer]))
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
        
        println(s"> loaded warc files from `${infile}'")
        return warcs
    }

    /** Gets the domain of a given url
     *  
     *  @param record the url to process
     *  @return the domain of a given url
     */
    def urlToDomain(url: String) : String = {
        val opt = Try { new URI(url).getHost() }.toOption
        return opt match {
            case None => ""
            case Some(null) => ""
            case Some(dom) => if (dom.startsWith("www.")) dom.substring(4) else dom
        }
    }

    /** Gets the source domain of a given record
     *  
     *  @param record the record to process
     *  @return the source domain of a given record
     */
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
    def tryGetYear(record: WarcRecord) : Int = {
        val headers = record.getHttpHeaders()
        var date = headers.get("Last-Modified")
        if (date == null) { date = headers.get("Date") }
        if (date == null) { return 0 }

        val getYear = "(\\d{4})".r
        val year = getYear.findFirstIn(date).getOrElse("0")
        return year.toInt
    }

    /** Gets all domains that were linked to by web pages
     *  along with the year they were linked to
     *  
     *  @param warcs the warc files to operate on
     *  @return a list of all linked domains
     */
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

        // save the RDD to a parquet file
        domains.toDF("domain", "year", "link")
            .write.partitionBy("year")
            .mode(SaveMode.Overwrite)
            .parquet(outfile)

        // finalize session
        sparkSession.stop()
    }
}
