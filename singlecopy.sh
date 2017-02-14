#run once to load the master data 
#bq rm -f nyt_singlecopy.singlecopy_all
#bq mk --schema singlecopy.json -t nyt_singlecopy.singlecopy_all
#bq load --skip_leading_rows=1  nyt_singlecopy.singlecopy_all singlecopy.csv

#bq rm -f nyt_singlecopy.singlecopy_bypass_all
#bq mk --schema singlecopy_bypass.json -t nyt_singlecopy.singlecopy_bypass_all
#bq load --skip_leading_rows=1  nyt_singlecopy.singlecopy_bypass_all singlecopy_bypass.csv

#!/bin/bash

#Get singleopy files from ftp server
result=`python /home/nimbul3/singlecopy/getFilesViaFTP.py`
echo $result

cutOffTime="200000"
currentTime=`date +"%H%M%S"`

if [ "$result" = 1 ]
then
   #No files received from the ftp server
   if [[ $currentTime -gt $cutOffTime ]]
   then
      echo "FAILED - processing of the Singlecopy files because no files were received by GMT Time  $currentTime" | mailx -S smtp=10.224.124.18:25 -r "saibal.patra@nytimes.com" -s "FAILED - processing of Singlecopy files" -v saibal.patra@nytimes.com
   fi
   echo "exiting since no files"
   exit 0
fi


FILE=""
DIR="/home/nimbul3/singlecopy/incoming/"

if [ "$(ls -A $DIR)" ]; then
     echo "Files have been received .. starting to process... "
     FILES=/home/nimbul3/singlecopy/incoming/*.TXT
     gsutil cp /home/nimbul3/singlecopy/incoming/*.TXT gs://nyt-singlecopy-bucket/incoming/
     for f in $FILES
     do 
         echo $f
         if [[ $f == *'DSS'* ]]; then
           name=$(basename "$f" "")
           echo "$name"
           echo "Need to clean file" $name
         sh clean_file.sh $name
         fi
         python /home/nimbul3/singlecopy/singlecopy.py $f
     done
     gsutil cp gs://nyt-singlecopy-bucket/incoming/*.TXT gs://nyt-singlecopy-bucket/processed/
     gsutil rm gs://nyt-singlecopy-bucket/incoming/*.TXT
     rm /home/nimbul3/singlecopy/incoming/*.TXT
fi


if [ "$?" != "0" ]; then
    echo "FAILED - processing of the Singlecopy files at GMT Time $currentTime" | mailx -S smtp=10.224.124.18:25 -r "saibal.patra@nytimes.com" -s "FAILED - processing of Singlecopy filies" -v saibal.patra@nytimes.com
    exit 1;
else
    echo "SUCCESS - processing of the Singlecopy files at GMT Time $currentTime" | mailx -S smtp=10.224.124.18:25 -r "saibal.patra@nytimes.com" -s "SUCCESS - processing of Singlecopy filies" -v saibal.patra@nytimes.com
    exit 0;
fi

