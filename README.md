# batch_ingestion
# Stage the package to Cloud Storage

gsutil cp target/scala-2.11/word-count_2.11-1.0.jar gs://${BUCKET_NAME}/scala/word-count_2.11-1.0.jar
    
# Process to run the jar into dataproc cluster
gcloud dataproc jobs submit spark \
    --cluster=${CLUSTER} \
    --class=dataproc.codelab.WordCount \
    --jars=gs://${BUCKET_NAME}/scala/word-count_2.11-1.0.jar \
    --region=${REGION} \
    -- gs://${BUCKET_NAME}/input/ gs://${BUCKET_NAME}/output/
    
 # sample spark submit
 gcloud dataproc jobs submit spark --
cluster=batch-ingestion 
--class=com.bip.spark.gcp.ReadFile 
--jars=gs://gcp_bip_ingestion_bucket/JARS/bd_bip_ingestion.jar 
--region=us-central1 -- gs://gcp_bip_ingestion_bucket/STAGING/ gs://gcp_bip_ingestion_bucket/RAW/ 20210615 gs://gcp_bip_ingestion_bucket/param/param.properties 

    
# View the output
gsutil cat gs://${BUCKET_NAME}/output/*
    
 
