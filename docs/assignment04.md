# Open Data

In this blog post we will take a look at the Pokémon database using Spark SQL and the Spark Dataframe API to see if we can find some interesting aspects of this dataset.

# Preparing the data

We start by downloading the dataset using the following shell commands.

```sh
mkdir -p /opt/hadoop/share/data
cd /opt/hadoop/share/data
echo Downloading Pokedex data to $(pwd)...
[ ! -f pokedex.csv ] \
    && wget --quiet https://raw.githubusercontent.com/JordyAaldering/Big-Data/master/Assignment04/data/pokedex.csv \
    && echo Downloaded Pokedex data || echo Pokedex data already exists
```

Then, using Spark, we can load this data into a dataframe. Here we will also cache it.

```scala
val pokedex_data = spark.read.format("csv")
    .option("header", true)
    .load("file:///opt/hadoop/share/data/pokedex.csv")
    .cache()
```

Lets now print this dataframe to get a feel of the data inside it. You have probably used `pokedex_data.show()` before, but we can actually print the data in a nicer way. The Zeppelin context provides its own command for pretty printing Spark dataframes: `z.show()`[[1](https://zeppelin.apache.org/docs/0.8.0/usage/other_features/zeppelin_context.html#exploring-spark-dataframes)].

```scala
z.show(pokedex_data.limit(5))
```

The Pokédex data has a lot of columns, so I'll only show the first few here.

![Raw data](https://raw.githubusercontent.com/JordyAaldering/Big-Data/master/Assignment04/images/raw-data.png)

Numerical data are still represented as strings. We could cast them later in SQL, but lets just do it now to save us some work and potential confusion later. For this we use the `withColumn` method on all columns that we need to cast. As an example the Pokédex number column is shown below.

```scala
val pokedex_data_cast = pokedex_data
    .withColumn("pokedex_number", col("pokedex_number").cast("int"))
    ...
```

We can now execute `pokedex_data_cast.printSchema()` to ensure that we cast everything correctly. The rest of this dataset is very well behaved, so this is all the pre-processing we have to do.

# Pokémon dual types

Every Pokémon has a certain type, and some Pokémon can even have two types. In the dataframe these are represented in the columns `type_1` and `type_2`, where the second one is null if the Pokémon only has one type.

Using SQL we can find out how many Pokémon with a certain first type two types. We can do this using the SQL command below. We also sort by the count of dual-types to make the output a bit easier to understand.

```sql
SELECT type_1, count(type_2) FROM pokedex_data_cast
    GROUP BY type_1
    ORDER BY count(type_2) DESC
```

This gives us the following results, which tells us that Pokémon whose first type is Bug of Water have the most secondary types.

![Dual type counts](https://raw.githubusercontent.com/JordyAaldering/Big-Data/master/Assignment04/images/dual-type-counts.png)

## Water dual types

I find water types more interesting, so lets find out what these dual water types actually are. To do so we first filter by rows where the first type is water, which we then group by the secondary type.

```sql
SELECT type_2, count(type_2) FROM pokedex_data_cast
    WHERE type_1 == "Water"
    GROUP BY type_2
    ORDER BY count(type_2) DESC
```

Because we are using Spark SQL we can show these results as a pie chart.

![Dual types water](https://raw.githubusercontent.com/JordyAaldering/Big-Data/master/Assignment04/images/dual-types-water.png)

This chart tells us that water-ground dual types occur most often. That's great, I love water-ground Pokémon!

Lets find out which Pokémon are these water-ground dual types.

```sql
SELECT name FROM pokedex_data_cast 
    WHERE type_1 == "Water" AND type_2 == "Ground"
```

![Water-ground types](https://raw.githubusercontent.com/JordyAaldering/Big-Data/master/Assignment04/images/water-ground-types.png)

## Water-ground effectiveness

Lets find out how strong these water-ground dual types are. The dataset contains a column `against_sometype` for each possible type. This is a decimal value which tells how effective a Pokémon is against that specific type.

To find out how effective other Pokémon are against water-ground types we will create a new column, which combines the values from `against_water` and `against_ground`. We can find this new value by simply multiplying these two columns.

```scala
val pokedex_against_wg = pokedex_data_cast
    .withColumn("against_water_ground", col("against_water") * col("against_ground"))
pokedex_against_wg.createOrReplaceTempView("pokedex_against_wg")
```

We can now count for each effectiveness value how many Pokémon are that effective against water-ground types. Here the `ORDER BY` is actually required because otherwise our bar chart (or equivalently; the line chart) will be in the wrong order. Especially for the line chart this will produce very strange results.

```sql
SELECT against_water_ground, count(against_water_ground) FROM pokedex_against_wg
    GROUP BY against_water_ground
    ORDER BY against_water_ground ASC
```

![Against water-ground](https://raw.githubusercontent.com/JordyAaldering/Big-Data/master/Assignment04/images/against-water-ground.png)

Finally, we can count how many times these values are greater or less than one to see how many Pokémon are strong or weak against water-ground types.

```sql
SELECT
    CASE WHEN against_water_ground > 1.0 THEN "Strong"
        ELSE CASE WHEN against_water_ground < 1.0 THEN "Weak" 
        ELSE "Normal"
    END END AS AgainstWater
FROM pokedex_against_wg
```

![Against water-ground counts](https://raw.githubusercontent.com/JordyAaldering/Big-Data/master/Assignment04/images/against-wg-counts.png)

Awesome, here we see that relatively few Pokémon are strong against water-ground types.

![Quagsire <3](https://media1.tenor.com/images/cce9a2b4083c6116d4fa873dd2a96028/tenor.gif?itemid=18674387)

---

The code shown in this blog can be found on GitHub [here](https://github.com/JordyAaldering/Big-Data/tree/master/Assignment04).
