echo 'upload branch rebalanced'
go build
gcloud compute scp sps-storm sps-storm-central:~/sps-storm