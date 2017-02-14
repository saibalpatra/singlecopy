from ftplib import FTP
import os
from datetime import datetime, time
import smtplib

server = smtplib.SMTP('10.224.124.18', 25)
from_addr = "saibal.patra@nytimes.com"
to_addr = "saibal.patra@nytimes.com"
now = datetime.now()
now_time = now.time()

def main():


    path="/home/nimbul3/singlecopy/incoming"
    os.chdir(path)
    ftp = FTP("ftp.nytimes.com")
    ftp.login('xcands','cess15')

    directory ="/cands/incoming"
    filematch = 'SB*.*' # a match for any file in this case, can be changed or left for user to input

    ftp.cwd(directory)

    for filename in ftp.nlst(filematch): # Loop - looking for matching files
      fhandle = open(filename, 'wb')
      ##print 'Getting ' + filename #for confort sake, shows the file that's being retrieved
      ftp.retrbinary('RETR ' + filename, fhandle.write)
      fhandle.close()

    filematch = "DSSBBYPFTP*.*"

    for filename in ftp.nlst(filematch): # Loop - looking for matching files
      fhandle = open(filename, 'wb')
      ##print 'Getting ' + filename #for confort sake, shows the file that's being retrieved
      ftp.retrbinary('RETR ' + filename, fhandle.write)
      fhandle.close()

    #if (os.listdir(path) == [] and now_time > time(19)):
       #message_text = "NO files found in ftp site. Please contact the single copy team"
       #subj = "NO files found in ftp site"
       #msg = "From: %s\nTo: %s\nSubject: %s\n\n%s" % ( from_addr, to_addr, subj, message_text )
       #server.sendmail(from_addr,to_addr, msg)
       #print 1
    #else:
       #print 0


    if (os.listdir(path) == []):
       print 1
    else:
       print 0

if __name__ == '__main__':
  main()
          
