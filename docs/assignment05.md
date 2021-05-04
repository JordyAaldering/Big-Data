# Streaming Data

In this notebook we will be learning about Spark Streaming by looking at a (fake) [RuneScape exchange](https://secure.runescape.com/m=itemdb_rs/).
This exchange will be simulated as a stream of orders which we will infinitely generate through our socket using a Python script.

## Loading in data

We start by creating a dataframe tied to the TCP/IP stream.

```scala
val socketDF = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .load()

socketDF.createOrReplaceTempView("runeUpdatesDF")
```

Using a regular expression we can now parse the input stream into two strings for the material and item name, and an integer for the sell price of that item.

```scala
val regex = "\"^([A-Z].+) ([A-Z].+) was sold for (\\\\d+)\""
val q = f"""SELECT 
        regexp_extract(value, $regex%s, 1) AS material, 
        regexp_extract(value, $regex%s, 2) AS item, 
        cast(regexp_extract(value, $regex%s, 3) AS Integer) AS price 
    FROM runeUpdatesDF"""
val runes = spark.sql(q)
```

## Initial analysis

To do some initial analysis on this data we will use a `writeStream` that writes the stream to memory.

```scala
val streamWriterMem = runes
    .writeStream
    .outputMode("append")
    .format("memory")
```

We can then run this writer for a few minutes to get some initial data.

```scala
val memoryQuery = streamWriterMem
    .queryName("memoryDF")
    .start()
    
// Run for 3 minutes
val t = 3 * 60 * 1000
memoryQuery.awaitTermination(t)
memoryQuery.stop()
```

Like in the previous blog post, we will use `z.show` to nicely plot dataframes and check if the data was read in correctly.

```scala
z.show(spark.sql("SELECT * FROM memoryDF").limit(10))
```

![Initial data](https://raw.githubusercontent.com/JordyAaldering/Big-Data/master/Assignment05/images/initial-data.png)

That looks correct. Now we can do some analysis to get a feel of the data. Lets first see how many rune items were sold in this time-span.

```sql
SELECT count(item) FROM memoryDF
    WHERE material = "Rune"
```

This tells us that 1833 rune items were sold.
We can also check how many of each item type was sold, we do this by grouping by item.

```sql
SELECT item, count(item) FROM memoryDF
    GROUP BY item
    ORDER BY count(item) DESC
```

![Item sales](https://raw.githubusercontent.com/JordyAaldering/Big-Data/master/Assignment05/images/item-sales.png)

Finally, lets see how much gold was spent in total on buying swords.

```sql
SELECT sum(price) FROM memoryDF
    WHERE item = "Sword"
```

Which shows that in total people spent 6526259 gold on buying swords.

# Streaming

Now that we have a feel of the data we will move on to using streaming to get rich in RuneScape. Using the initial data we just computed we are going to compute the average price of each item, and then when someone lists an item below this average, we will buy it so that we can sell it at the average price and earn a profit.

## Average item prices

To compute the average prices we will combine the item and its material into a single entry and group by that entry, where each group will contain the average price of that item.

```scala
var avg_prices = spark.sql("""
    SELECT concat(material, " ", item) as item, 
           count(item) AS amount, 
           cast(avg(price) AS Int) AS average 
    FROM memoryDF
        GROUP BY item, material
        ORDER BY avg(price) DESC
""")

z.show(avg_prices.limit(10))
```

![Average prices](https://raw.githubusercontent.com/JordyAaldering/Big-Data/master/Assignment05/images/avg-prices.png)

As you see we also keep track of the amount of each item. We won't be using it in this blog post, but an improved version of this code could use this amount to repeatedly update the averages using the streaming data.

## Buy and sell loop

Now that we know the averages, we can look at each new entry into the exchange. Then when the price of an item is below average we will buy it and immediately sell it at a profit.
Below we define a function that decides to buy a listing if its price is below that of the average. This listing is a row that contains the item name and its listed price.

```scala
def buyListingAtProfit(listing: Row) = {
    val avgPrices = avg_prices.filter(s"item = '${listing(0)}'").select("average")
    if (avgPrices.count > 0) {
        val sellPrice: Int = listing(1).asInstanceOf[Int]
        val avgPrice = avgPrices.first()(0).asInstanceOf[Int]
        if (sellPrice < avgPrice) {
            println("Buying item " + listing(0) + " with a profit of " 
                + (avgPrice - sellPrice) + " gold")
        }
    }
}
```

Since this is just an example, we will just print a message to show that we made some profit.

We are going to apply this function by using the [`foreachBatch`](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#foreachbatch) method on the stream.
This method applies the function to each incoming batch of the streaming data. This batch is a dataset since it might contain multiple listings.

```scala
def processBatch(batchDF: Dataset[Row]) = {
    val listings = batchDF.select("item", "price").collect()
    for (listing <- listings) {
        buyListingAtProfit(listing)
    }
}

val runeStream = runes
    .writeStream
    .outputMode("append")
    .format("console")
    .foreachBatch{ (batchDF: Dataset[Row], batchId: Long) => {
        processBatch(batchDF)
    }}
    .start()
```

This is kind of a hack, but using `exception` we can print our log messages.

```scala
for (a <- 1 to 1000) {
    val exc = runeStream.exception
    if (exc != None) {
        println(exc)
    }
    Thread.sleep(10)
}
```

```
Buying item Mithril Battleaxe with a profit of 10 gold
Buying item Dragon Hasta with a profit of 93 gold
Buying item Mithril Hatchet with a profit of 90 gold
Buying item Iron Hasta with a profit of 16 gold
Buying item Adamant Halberd with a profit of 46 gold
```

Awesome, our code seems to be working and we are making a profit!
Don't forget to stop the stream when you are done.

```scala
runeStream.stop()
```

---

The code shown in this blog can be found on GitHub [here](https://github.com/JordyAaldering/Big-Data/tree/master/Assignment05).
