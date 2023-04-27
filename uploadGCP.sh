echo 'upload branch predictive-extension'
go build
gcloud compute scp sps-storm sps-storm-central:~/sps-storm
