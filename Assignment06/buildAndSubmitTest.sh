sbt assembly &&
spark-submit \
    --deploy-mode cluster \
    --num-executors 10 \
    --queue silver \
    target/scala-2.12/LinkedDomainsCounter-assembly-1.0.jar \
    /single-warc-segment/CC-MAIN-20210410105831-20210410135831-00639.warc.gz \
    /user/JordyAaldering/out
