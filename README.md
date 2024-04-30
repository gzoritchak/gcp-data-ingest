# data ingest
This project is the support of the talk about ingesting data with Kotlin and coroutines.


### Scripts

#### application default login
```bash
gcloud auth login --update-adc 
```

### upload file using gsutil
```bash
gsutil cp generated_200iMB.csv gs://d2v-ingest-demo
```

### download file using gsutil
```bash
time gsutil cp gs://d2v-ingest-demo/generated_200MiB.csv ./temp/generated_200MiB.csv
```

