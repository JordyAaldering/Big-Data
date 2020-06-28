---
layout: post
title: "BDR Hackaton"
date: 2020-05-15
---

Randstad is the biggest Dutch employment agency that processes thousands of vacancies every month. In this blog post we will be building a recommender system that recommends vacancies to candidates and vice versa.

---

Before we can get started on building the recommender itself we need to take a look at the data and clean it up if necessary. We can use ```df.show()``` to print the first few rows of a certain dataset, this gives us an idea of what columns there are and how data is represented in them. Next we can also use ```df.describe()``` to give us other useful inforation about the columns like the number of rows, the mean and standard deviation, and the minimum and maximum.

After doing this we see that the 'ecom_action' column in the 'clicks_val' DataFrame is always a value between one and six. Perhaps we can simplify this data further. We can print the counts of each value with the following code:

```scala
clicks_val.groupBy("ecom_action")
          .count()
          .show()
```
```bash
+-----------+-----+
|ecom_action|count|
+-----------+-----+
|          1|    1|
|          6| 4355|
|          3|   34|
|          5|  973|
|          2| 6387|
+-----------+-----+
```

We see that there is only a single one and only 34 threes. Therefore we will change all ones into twos and all threes and fives into fours. We do this by making a UDF (_User Defined Function_) and applying it on the DataFrame.

```scala
def ecomActionRemap(s: String): Option[Integer] = {
  var i = 0;
  try {
    i = s.toInt;
  } catch {
    case e: Exception => Some(-1);
  }
  
  if (i == 1) {
    Some(2)
  } else if (i == 3 || i == 5) {
    Some(4)
  } else {
    Some(i);
  }
}

val tRemap = udf((f: String) => ecomActionRemap(f))
```

---

Now that the data is cleaned up we can start training our recommender. For this we want to perform matrix factorization using the Alternative Least Squares algorithm. Luckily, Scala has a library for this making it easy to implement. Lets get started by making the ALS:

```scala
val als = new ALS()
  .setMaxIter(6)
  .setRegParam(1)
  .setAlpha(1)
  .setUserCol("candidate_number")
  .setItemCol("vacancy_number")
  .setRatingCol("ecom_action")
```

We can now create our model using this ALS and the train data. We also need to set the cold-start strategy to 'drop' to avoid NaN values for candidates or vacancies that have no rating history yet and on which the model has not been trained.

```scala
val model = als.fit(clicks_train)
model.setColdStartStrategy("drop")
```

We can get the Root Mean Square Error of our model by using Spark's regression evaluator:

```scala
val evaluator = new RegressionEvaluator()
  .setMetricName("rmse")
  .setLabelCol("ecom_action")
  .setPredictionCol("prediction")

val rmse = evaluator.evaluate(predictions)
println("Root Mean Square Error: " + rmse)
```

```bash
Root Mean Square Error: 2.24
```

---

We now have a working recommender! However there is an important issue with the way we have set up our model: the recommender can only recommend vacancies that were present in the training data. Vacancies come and go very quickly, so this is clearly something we need to fix. To do this we need to find a way for our recommender to recommend new vacancies based on old ones. We do this by looking at the context of a vacancy, so that we can make comparisons between similar vacancies, instead of looking at each vacancy as a separate entity. We can use the 'function_name' value included in candidate-vacancy pairs for this.

Before we can continue we also need a grouping of candidate numbers and function indices, along with their corresponding sums of 'ecom-action's. We can now create another ALS and model in the same manner as we did before:

```scala
val als = new ALS()
  .setMaxIter(6)
  .setRegParam(1)
  .setAlpha(1)
  .setUserCol("candidate_number")
  .setItemCol("function_index")
  .setRatingCol("sum(ecom_action)")

val model = als.fit(grouped_train)
model.setColdStartStrategy("drop")
```

---

All features are now present in our large table, however the values differ a lot between different features, therefore we need to normalize the distance, hour wage, and function prediction so that we get a value between 0 and 1. First we get the maximum distance:

```scala
val max_distances = vacancies_distance
  .groupBy("candidate_number")
  .agg(max($"distance").alias("max_distance"),
       max($"request_hour_wage").alias("max_request_hour_wage"),
       max($"prediction").alias("max_prediction"))
```

Which we can then use to normalize the data. For the distance we need to keep in mind that a lower distance is better, so we subtract its normalized value from 1 to inverse it.

```scala
var vacancies_max_joined = vacancies_distance
    .join(max_distances, Seq("candidate_number"), "inner")

vacancies_max_joined = vacancies_max_joined.withColumn(
    "normalized_distance",
    lit(1) - col("distance").divide(col("max_distance")))

vacancies_max_joined = vacancies_max_joined.withColumn(
    "normalized_prediction",
    col("prediction").divide(col("max_prediction")))

vacancies_max_joined = vacancies_max_joined.withColumn(
    "normalized_request_hour_wage",
     col("request_hour_wage").divide(col("max_request_hour_wage")))
```

---

When evaluating our predictions by running our ALS on the original dataset we get an accuracy of about 6.13%. There are some relatively simple things we can do to improve this accuracy. For instance, we could include whether the number of hours is in range of a candidates' requested hours. The same could be done for other values like the wage or distance.

When we evaluate our predictions again with the provided test data we get an accuracy of 2.91%.

---

The source code of this project can be found on [GitHub](https://github.com/JordyAaldering/Big-Data/tree/master/Assignment04). <br>
Thank you for reading! 🙃
