echo 'upload branch dev'
go build
gcloud compute scp sps-storm sps-storm-central-backup-1:~/sps-storm
