Singlecopy Process
==================

## The process
The process has been scheduled as a cron job which runs every 30 min on Wednesdays from 7am to 3pm
If no files are found by 3pm, an error message is sent to Ann and the DW team

*/30 12-20 * * 3 /home/nimbul3/singlecopy/singlecopy.sh &> /tmp/singlecopy_cronlog.txt

