cd job
gcloud builds submit --pack image=us-docker.pkg.dev/peace-demo/docker/gen-user-stat-job
gcloud run jobs update job-quickstart \
    --image us-docker.pkg.dev/peace-demo/docker/gen-user-stat-job \
    --tasks 1 \
    --set-env-vars SLEEP_MS=1000 \
    --max-retries 3 \
    --region us-central1 \
    --project=peace-demo \
    --set-cloudsql-instances=peace-demo:us-central1:meitu-demo-2307 \
    --set-env-vars=INSTANCE_CONNECTION_NAME=peace-demo:us-central1:meitu-demo-2307 \
    --set-env-vars=DB_USER=db \
    --set-env-vars=DB_PASS=ping@#\$1234 \
    --set-env-vars=DB_NAME=meitu_db

gcloud sql instances patch meitu-demo-2307 --enable-bin-log
