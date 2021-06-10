package org.rubigdata

import org.apache.hadoop.io.NullWritable
import de.l3s.concatgz.io.warc.{WarcGzInputFormat,WarcWritable}
import de.l3s.concatgz.data.WarcRecord

import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

import org.jsoup.Jsoup
import org.jsoup.nodes.Document

import collection.JavaConverters._
import collection.mutable.{Buffer,ArrayBuffer}
import java.net.URI
import util.Try

object LinkedDomainsCounter {
    /** Used for keeping all log messages in one place */
    object Log {
        val log = new ArrayBuffer[String]()

        def Print(msg: String) {
            log += msg
        }

        def Show() {
            println("=" * 50)
            log.foreach(println)
            println("=" * 50)
        }
    }

    def setupSparkSession() : SparkSession = {
        val sparkConf = new SparkConf()
            .setAppName("LinkedDomainsCounter")
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .registerKryoClasses(Array(classOf[WarcRecord]))
        val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
        Log.Print(s"> running spark version ${sparkSession.version}")
        return sparkSession
    }

    /** Loads warcs from a local file
     *  
     *  @param filename the name of the warc file without the extension
     *  @param sc the spark context
     *  @return the loaded warcs as an RDD
     */
    def loadWarcs(filename: String, sc: SparkContext) : RDD[(NullWritable, WarcWritable)] = {
        val filepath = s"file:///opt/hadoop/rubigdata/${filename}.warc.gz"
        val warcs = sc.newAPIHadoopFile(
                filepath,
                classOf[WarcGzInputFormat],
                classOf[NullWritable],  // Key
                classOf[WarcWritable]   // Value
            )
            .cache()
        
        Log.Print(s"> loaded ${warcs.count()} warc files from `${filepath}'")
        return warcs
    }



    /** Tries to get the last modified date of a record
     *  
     *  @param record the record to process
     *  @return the last modified date of a record
     */
    def tryGetYear(record: WarcRecord) : Int = {
        val headers = record.getHttpHeaders()
        var date = headers.get("Last-Modified")
        if (date == null) {
            date = headers.get("Date")
        }
        
        if (date != null) {
            return date.substring(12, 16).toInt
        }
        
        date = record.getHeader().getDate()
        if (date != null) {
            return date.substring(0, 4).toInt
        }

        return -1
    }

    /** Removes the html tag `< />' or `< >' around a domain
     *  
     *  @param dom the domain name to clean
     *  @return the, possibly unchanged, domain name
     */
    def removeHtmlTag(dom: String) : String = {
        if (dom.startsWith("<")) {
            if (dom.endsWith("/>")) {
                return dom.substring(1, dom.length() - 2)
            } else if (dom.endsWith(">")) {
                return dom.substring(1, dom.length() - 1)
            }
        }

        // there was no tag to remove
        return dom
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

    /** Finds all domains that were linked using hrefs of a given html body
     *  
     *  @param body a Jsoup parsed body of an html document
     *  @return a list of all domains linked by the given html
     */
    def getHrefDomains(body: Document) : Buffer[String] = {
        val hrefs = body.select("a[href]").asScala.map(_.attr("href"))
        return hrefs.map(urlToDomain)
    }

    /** @return whether the given strings are empty or equal */
    def emptyOrEqual(a: String, b: String) : Boolean = {
        return a == "" || b == "" || a == b
    }

    /** Gets all domains that were linked to by web pages
     *  
     *  @param warcs the warc files to operate on
     *  @return a list of all linked domains
     */
    def getAllDomains(warcs: RDD[(NullWritable, WarcWritable)]) : RDD[(Int, String)] = {
        val sTime = System.nanoTime

        val domains = warcs
            // get all http records
            .map(_._2.getRecord())
            .filter(_.isHttp())
            .filter(_.getHeader().getUrl() != null)
            // split records into the file's source url and http body
            .map { record =>
                val url = record.getHeader().getUrl()
                val year = tryGetYear(record)
                (url, year) -> record.getHttpStringBody()
            }
            // get all linked domains
            .flatMap { case ((url, year), body) => {
                val links = getHrefDomains(Jsoup.parse(body))
                val dom = urlToDomain(removeHtmlTag(url))
                links.filter(l => !emptyOrEqual(dom, l))
                    .map(l => (year, l))
            }}
            .cache()
        
        val duration = (System.nanoTime - sTime) / 1e6d
        Log.Print(s"> found ${domains.count()} linked domains in total"
                + s" (${Math.round(duration)} ms)")
        return domains
    }

    /** Uses map-reduce to count how many times each domain was linked
     *  
     *  @param domains a list of all domains that were linked
     *  @return a mapping of domains to how many times were linked
     */
    def reduceDomains(domains: RDD[(Int, String)]) : RDD[((Int, String), Int)] = {
        val sTime = System.nanoTime

        val mapped = domains.map(_ -> 1)
        val reduced = mapped.reduceByKey(_ + _).cache()
        
        val duration = (System.nanoTime - sTime) / 1e6d
        Log.Print(s"> found ${reduced.count()} unique linked domains"
                + s" (${Math.round(duration)}ms)")
        return reduced
    }



    /** Prints the domains that were linked the most
     *  
     *  @param counts a mapping of domains to how many times were linked
     *  @param n the maximum number of domains to print
     */
    def printTopDomains(counts: RDD[((Int, String), Int)], n: Int) {
        Log.Print(s"> showing top ${n} domains")
        counts.takeOrdered(n)(Ordering[Int].reverse.on(_._2))
            .foreach { case ((year, dom), num) =>
                Log.Print(s"in ${year}, ${dom} is linked ${num} times")
            }
    }

    /** Prints how many times a domain was linked the past years
     *  
     *  @param counts a mapping of domains to how many times were linked
     *  @param dom: the domain to print the yearly links of
     *  @param n the maximum number of years to print
     */
    def printYearlyLinks(counts: RDD[((Int, String), Int)], dom: String, n: Int) {
        Log.Print(s"> showing ${dom} links per year the past ${n} years")
        counts.filter(_._1._2 == dom)
            .takeOrdered(n)(Ordering[Int].reverse.on(_._1._1))
            .foreach { case ((year, _), num) =>
                Log.Print(s"in ${year}, ${dom} was linked ${num} times")
            }
    }

    def main(args: Array[String]) {
        // check arguments
        if (args.length < 1 || args.length > 4) {
            println("> usage: LinkedDomainsCounter <filename> "
                + "<numPrintedDomains=10> "
                + "<domainToCheckYearly='facebook.com'> "
                + "<numPrintedYears=10>")
            return
        }

        // parse arguments
        val filename = args(0)
        val numPrintedDomains = if (args.length > 1) args(1).toInt else 10
        val domainToCheckYearly = if (args.length > 2) args(2) else "facebook.com"
        val numPrintedYears = if (args.length > 3) args(3).toInt else 10

        // start session
        val sparkSession = setupSparkSession()
        val sc = sparkSession.sparkContext

        // compute linked domain counts
        val warcs = loadWarcs(filename, sc)
        val domains = getAllDomains(warcs)
        val counts = reduceDomains(domains)

        // print results
        printTopDomains(counts, numPrintedDomains)
        Log.Print("")
        printYearlyLinks(counts, domainToCheckYearly, numPrintedYears)

        // finalize session
        sparkSession.stop()
        Log.Show()
    }
}
