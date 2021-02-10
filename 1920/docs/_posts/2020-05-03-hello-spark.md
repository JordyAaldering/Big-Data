---
layout: post
title: "Hello Spark"
date: 2020-05-03
---

In this blog post we will be analyzing public data provided by the city of Nijmegen using Docker and Spark. We will start by following the provided assignment and then expanding from there. First lets take a look at the BAG dataset, this dataset contains records of addresses and buildings in Nijmegen. Using Spark's ```describe``` method we can get a quick overview of the data, this also allows us to see possible mistakes in the formatting. For instance; the output shows that there is no mean for the _x coördinates_. This of course makes no sense and indicates an error in the data.

We would like to fix errors like these; but how? It is often hard to figure out the best way to fix these kinds of errors. We might just want to remove troublesome fields, but if there are many we might make our sample size too small. Another common approach is to choose the mean or average for missing data points. In the case above I decided to remove the missing data points, as there are only a few and taking the mean or average would make less sense in this particular case.

After removing all null values and describing the data again the mean still does not show up, why is this? When we look at the coördinates we can see that they are written as _0,01_ instead of the expected _0.01_. Luckily, this can easily be fixed by first replacing commas with dots using a number formatter, and then casting them to floats.

---

Next up we'll take a look at Nijmegen's artworks. We'll select all artworks that have been created before 2000 using SQL-like syntax. This syntax allows us to easily select data with certain properties and values.

```scala
artworks.select("naam", "locatie", "latitude",
                "longitude", "bouwjaar", "url")
        .where("bouwjaar <= 1999")
```

We again see that floats are formatted incorrectly, so we'll apply the same technique as before. Upon further inspection we notice that there are more problems with the data. The mean of the build year is **2043?** That doesn't make sense! When showing some data with a year above 2020 we see that many buildings have build year _9999_. This is likely to indicite that the year is unknown. We'll be replacing these values with nulls.

---

We can now start actually working with the data! It would be nice if we could place the data from the different datasets on a virtual map, the problem is that they are in different coördinate systems. Luckily there exists an easy to use Java library, [CTS](https://github.com/orbisgis/cts), that allows us to convert between these different systems.

Now lets go ahead and finally plot some data. First we'll get a smaller subset containing only the Toernooiveld street:

```scala
addr.filter('street === "Toernooiveld")
    .select('street, 'x, 'y, 
        'latlon .getField("_1") as "lat",
        'latlon .getField("_2") as "lon")
```

And then we'll show it on a map using the neat widgets provided by spark notebook:

```scala
GeoPointsChart(latlon.select('lat, 'lon)
```

![Map of Toernooiveld](https://raw.githubusercontent.com/JordyAaldering/Big-Data/master/Assignment03/images/mapToernooiveld.png)

---

Now lets do something fun with this data; we are going to plot artworks depending on their build year. This will give us an impression of how Nijmegen has grown throughout the years. Firstly we need to combine the two datasets so that we have access to both the build year, and the latitude and longitude. Doing this is easy:

```scala
val kos = ks
  .withColumn("lat", translate(ks.col("lat"), ",", ".")
    .cast("float"))
  .withColumn("lon", translate(ks.col("lon"), ",", ".")
    .cast("float"))
  .withColumn("year", col("year").cast("int"))
  .select("name", "location", "year", "lat", "lon")
```

We can now see where artworks have been made in a certain range of years. Let's take a look at an example:

```scala
val k = kos.filter('year >= 2000 and 'year < 2010)
GeoPointsChart(k.select('lat, 'lon))
```

![Map of 2000-2010](https://raw.githubusercontent.com/JordyAaldering/Big-Data/master/Assignment03/images/map2000.png)

At this point I wanted to plot all artworks in a single chart, with a color corresponding to the build year. I know this has something to do with the ```rField``` and ```colorField``` parameters of the geo points chart. But sadly the documentation on this tool is very limited and I could thus not find out how to get it to work correctly.

---

The source files and products of this assignment can be found on my [GitHub](https://github.com/JordyAaldering/Big-Data/tree/master/Assignment03) repository. Thank you for reading! 🙃
