sbt assembly &&
spark-submit \
    --deploy-mode cluster \
    --num-executors 20 \
    --queue gold \
    target/scala-2.12/LinkedDomainsCounter-assembly-1.0.jar \
    /single-warc-segment \
    /user/JordyAaldering/out
