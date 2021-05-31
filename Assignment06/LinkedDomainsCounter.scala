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
            ).cache()
        Log.Print(s"> loaded ${warcs.count()} warc files from `${filepath}'")
        return warcs
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
     *  @param allowSelfLinks whether links to the same domain as the source are counted
     *  @return a list of all linked domains
     */
    def getAllDomains(warcs: RDD[(NullWritable, WarcWritable)], allowSelfLinks : Boolean = false) : RDD[String] = {
        val sTime = System.nanoTime

        val domains = warcs
            // get all http records
            .map(_._2.getRecord())
            .filter(_.isHttp())
            // split records into the file's source url and http body
            .map(r => r.getHeader().getUrl() -> r.getHttpStringBody())
            .filter { case (url, _) => allowSelfLinks || url != null }
            // get all linked domains with a filter depending
            // on whether self-links are allowed
            .flatMap { case (url, body) => {
                val links = getHrefDomains(Jsoup.parse(body))
                if (allowSelfLinks) {
                    links.filter(_ != "")
                } else {
                    val t = urlToDomain(removeHtmlTag(url))
                    links.filter(l => !emptyOrEqual(t, l))
                }
            }}.cache()
        
        val duration = (System.nanoTime - sTime) / 1e6d
        Log.Print(s"> found ${domains.count()} linked domains in total,"
                + s" ${if (allowSelfLinks) "with" else "without"} self-links"
                + s" (${Math.round(duration)} ms)")
        return domains
    }

    /** Uses map-reduce to count how many times each domain was linked
     *  
     *  @param domains a list of all domains that were linked
     *  @return a mapping of domains to how many times were linked
     */
    def reduceDomains(domains: RDD[String]) : RDD[(String, Int)] = {
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
    def printTopDomains(counts: RDD[(String, Int)], n: Int = 20) {
        Log.Print(s"> showing top ${n} domains")
        counts.takeOrdered(n)(Ordering[Int].reverse.on(_._2))
            .foreach { case (dom, num) =>
                Log.Print(s"${dom} is linked ${num} times")
            }
    }

    def main(args: Array[String]) {
        // check arguments
        if (args.length < 1 || args.length > 3) {
            println("> usage: LinkedDomainsCounter <filename> "
                + "<allowSelfLinks=false> <numPrintedDomains=20>")
            return
        }

        // parse arguments
        val filename = args(0)
        val allowSelfLinks = args.length == 2 && args(1).toLowerCase() == "true"
        val numPrintedDomains = if (args.length == 3) args(2).toInt else 20

        // start session
        val sparkSession = setupSparkSession()
        val sc = sparkSession.sparkContext

        // compute linked domains
        val warcs = loadWarcs(filename, sc)
        val domains = getAllDomains(warcs, allowSelfLinks)
        val counts = reduceDomains(domains)
        printTopDomains(counts, numPrintedDomains)

        // finalize session
        sparkSession.stop()
        Log.Show()
    }
}
