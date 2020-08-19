export COMPOSER_BUCKET=<bucket/dags>
gsutil cp bq-testing.py $COMPOSER_BUCKET
gsutil cp scripts/*  $COMPOSER_BUCKET/scripts
gsutil cp conf/*  $COMPOSER_BUCKET/conf