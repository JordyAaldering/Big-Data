# Sparkling Spark

In this blog post we will use Spark and RDDs (Resilient Distributed Datasets) to find interesting information about co-occurrences in [Project Gutenberg’s The Complete Works of William Shakespeare](https://raw.githubusercontent.com/rubigdata-dockerhub/hadoop-dockerfile/master/100.txt).
To do so we will use a docker container, running a Zeppelin notebook.

## Pre-processing the input

We start by loading the downloaded file as an RDD.

```scala
val lines = sc.textFile("file:///opt/hadoop/100.txt")
```

This creates an RDD of strings, where each of these strings is a single line of the input file.
We now need to split these lines into separate words. To do so we first split each line at places where there is a space, after which we clean up these words be removing special characters and by making everything lowercase. Finally we remove all empty words.

```scala
// remove special characters and make all lower case
val cleanWord = (w: String) =>
    w.replaceAll("\\W", "").toLowerCase()

val lines2d =
    lines.map(
        _.split(" ")
         .map(cleanWord)
         .filter(_ != "")
    )
```

If you're running this code you might notice that it completes unexpectedly quickly. This is because Spark is lazily evaluated, which means that expressions are only evaluated when their resulting values are needed. Up until this point we have not yet requested any results, so the values of, for instance, `lines2d` have actually not been computed yet.

Now we'll change that; let's see if `lines2d` looks as expected by printing the first few lines. We should see the original lines of the text without special characters and with commas in between all words.

```scala
lines2d.take(10).map(
    arr => printf("%s\n", arr.mkString(", "))
)
```
```
project, gutenbergs, the, complete, works, of, william, shakespeare, by
william, shakespeare

this, ebook, is, for, the, use, of, anyone, anywhere, in, the, united, states, and
most, other, parts, of, the, world, at, no, cost, and, with, almost, no
restrictions, whatsoever, you, may, copy, it, give, it, away, or, reuse, it
under, the, terms, of, the, project, gutenberg, license, included, with, this
ebook, or, online, at, wwwgutenbergorg, if, you, are, not, located, in, the
united, states, youll, have, to, check, the, laws, of, the, country, where, you
```

## Co-occurrences

Now that each line is an array of words we can start counting the co-occurrences. We will say that two words co-occur if they appear together in a line.

To find these co-occurrences we will use a `flatMap`. This map will take all possible combinations of the words in a line, and will append them together with a colon.
These combinations are then placed in a 1d list inside a tuple with a 1, so that we can process this data using Map-Reduce.

```scala
val cooccurences =
    lines2d.flatMap(
        _.combinations(2)
         .map(_.mkString(":") -> 1)
    )
```

Again we print these values to check if we did everything correctly.

```scala
cooccurences.take(10).map {
    case(k,v) => printf("%s, %s\n", k, v)
}
```
```
project:gutenbergs, 1
project:the, 1
project:complete, 1
project:works, 1
project:of, 1
project:william, 1
project:shakespeare, 1
project:by, 1
gutenbergs:the, 1
gutenbergs:complete, 1
```

That looks correct.
We can now use reduce on this RDD by counting how many times each word-word pair occurs. We also cache this result to memory to avoid having to compute this value multiple times when we evaluate the results later.

```scala
val co = cooccurences.reduceByKey(_ + _)
co.cache()
```

## Results

Let's first take a look at the ten most occurring pairs to see if everything looks correct. We order everything by a decreasing number of occurrences, and print the first few values.

```scala
val coTop = co.takeOrdered(10)(Ordering[Int].reverse.on(_._2))

coTop.map {
    case(k,v) => printf("%s\toccurs %d times\n", k, v)
}
```
```
the:of      occurs 6318 times
and:the     occurs 3300 times
the:the     occurs 3205 times
to:the      occurs 3086 times
the:and     occurs 2579 times
and:to      occurs 2295 times
i:you       occurs 2260 times
i:to        occurs 2228 times
and:of      occurs 2189 times
i:have      occurs 2173 times
```

This looks correct, but obviously these results are not very interesting. We can also take a specific word like _Romeo_, and check what words it often co-occurs with.

To do this we first need to split the key back up in two words, and check if either of those two is the word we want. After which we can find the top ten values like before.

```scala
def occurrencesOf(word: String, amount: Int) {
    val top = co.filter {
        case(k,v) => k.split(":") contains word
    }.takeOrdered(amount)(Ordering[Int].reverse.on(_._2))
    
    top.map {
        case(k,v) => printf("%s\toccurs %d times\n", k, v)
    }
}

occurrencesOf("romeo", 10)
```
```
and:romeo   occurs 19 times
enter:romeo occurs 13 times
romeo:and   occurs 12 times
to:romeo    occurs 10 times
the:romeo   occurs 9 times
romeo:he    occurs 9 times
romeo:to    occurs 9 times
romeo:is    occurs 9 times
i:romeo     occurs 8 times
romeo:a     occurs 8 times
```

If we print a few more values we see that _Romeo_ and _Juliet_ only co-occur 6 times. This might seem like very little, but keep in mind that we look at the co-occurrences within lines, and not sentences.
An improved version of this code would look at entire sentences instead of lines, which might provide more interesting results.

### O Romeo, Romeo, wherefore art thou Romeo?

As I was looking for some interesting co-occurrences I thought of the famous quote "O Romeo, Romeo, wherefore art thou Romeo?", and I decided to look up the co-occurrences of _thou_, I thought it was pretty funny that _art_ and _thou_ co-occur so often.

```
thou:art    occurs 585 times
thou:the    occurs 531 times
thou:to     occurs 525 times
thou:me     occurs 518 times
and:thou    occurs 518 times
thou:not    occurs 472 times
thou:a      occurs 468 times
thou:thy    occurs 453 times
thou:and    occurs 448 times
thou:thou   occurs 427 times
```

---

The [code](https://github.com/JordyAaldering/Big-Data/tree/master/Assignment03) provided in this blog can be found on GitHub.
