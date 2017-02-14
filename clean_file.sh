gsutil cp gs://nyt-singlecopy-bucket/incoming/"$1" .
sed 's/\"//g'   "$1" >   "$1".NEW.TXT
sed 's/,//3'   "$1".NEW.TXT >   "$1".NEWER.TXT
cp "$1".NEWER.TXT /home/nimbul3/singlecopy/incoming/"$1"
gsutil cp "$1".NEWER.TXT  gs://nyt-singlecopy-bucket/incoming/"$1" 
rm "$1"
rm "$1".NEW.TXT
rm "$1".NEWER.TXT
