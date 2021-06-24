spark-submit \
    --deploy-mode cluster \
    --num-executors 20 \
    --queue gold \
    target/scala-2.12/ReduceDomains-assembly-1.0.jar \
    /user/JordyAaldering/out \
    /user/JordyAaldering/reduced \
    100
