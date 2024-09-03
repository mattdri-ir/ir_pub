#!/usr/bin/python


__desc__= "pffexport push to elasticsearch"
__email__ = "mattdri@gmail.com"

import argparse
import sys
import os
import re
import json
import glob
from elasticsearch import Elasticsearch, helpers
from bs4 import BeautifulSoup


import hashlib
from dateutil.parser import parse




argv = sys.argv
        
parser = argparse.ArgumentParser(description="pffexport to elasticsearch")

parser.add_argument("-e", "--endpoint",required=True, help="elastic host https://172.17.0.1:9200")
parser.add_argument("-u", "--user", required=True, help="elastic username")
parser.add_argument("-p", "--password", required=True,  help="elastic password")
parser.add_argument("-i", "--index", required=True, help="elastic index")
parser.add_argument("-d", "--directory",required=True,  help="pffexport directory")

args = parser.parse_args(argv[1:])

host = args.endpoint
http_auth = (args.user,args.password)




messages = {}

es = Elasticsearch(
    hosts = [host],
    http_compress = True, # enables gzip compression for request bodies
    verify_certs = False,
    ssl_assert_hostname = False,
    ssl_show_warn = False,
    basic_auth=http_auth
)

def sha256_checksum(filename, block_size=65536):
    sha256 = hashlib.sha256()
    with open(filename, 'rb') as f:
        for block in iter(lambda: f.read(block_size), b''):
            sha256.update(block)
    return sha256.hexdigest()

def process_messages(dir):
    item = { 'data_type': 'pffexport' , "directory": dir}
    _id=None
    print(dir)

    for path in glob.glob(dir+"/*"):
            
            emailsize = 0

            #check attachments before directorty recursion
            if os.path.basename(path) == 'Attachments':
                attachments = []
                for x in glob.glob(path+"/*"):
                    if os.path.isdir(x) == False:
                        attachments.append({'sha256':sha256_checksum(x),'name':os.path.basename(x)})
                if len(attachments) > 0:
                    item['attachments'] = attachments
            #directory recursion
            if os.path.isdir(path):
                process_messages(path)

            #Recipeitns is a list
            if os.path.basename(path) == "Recipients.txt":
                recipients = []
                recipient = {}
                for line in open(path):
                    m = re.match("([^:]+):\s+(.+)",line)
                    if m:

                        if m.group(1).lower() == 'display name':
                            if 'display name' in recipient:
                                recipients.append(recipient)
                                recipient = {}
                            recipient[m.group(1).lower()] = m.group(2)
                        else:
                            recipient[m.group(1).lower()] = m.group(2)
                if 'display name' in recipient:
                    recipients.append(recipient)
                item['recipients'] = recipients

            #Rip all the headers into fields
            if os.path.basename(path) in ["OutlookHeaders.txt","Appointment.txt"]:
                 
                f = open(path)
                info = ""
                for line in f:
                    info += line
                    m = re.match("([^:]+):\s+(.+)",line)
                    if m:
                        item[m.group(1).lower()] = m.group(2)

            #Using Outlook headers as the unique ID. Internet Headers don't exist for internal docs
            if os.path.basename(path) == "OutlookHeaders.txt":
                _id=sha256_checksum(path)

            #Doesn't seem to mean a lot right now but storing just in case. Not handling Filetime.
            if os.path.basename(path) == "ConversationIndex.txt":
                for x in open(path):
                    if "GUID" in x:
                        item['conversation_guid']= x.split("\t")[-1]

                
            #Rip all Internet Headers for now
            if os.path.basename(path) == "InternetHeaders.txt":
                        
                emailsize = os.path.getsize(path)
                value = None
                f = open(path)
                
                line = "foo"
                k= ""

                while(len(line) > 0):
                    line = f.readline()
    
                    if re.match("^ .+",line):
                        item[k] = item[k] + line
                    else:
                        m = re.match("([^:]+): (.+)",line)
                        if m:
                            k = m.group(1).lower()
                            item[k] = m.group(2)
                        else:
                            #handle key without val on single line
                            m = re.match("([^:]+):",line)
                            if m:
                                k = m.group(1).lower()
                                item[k] = ""

            #Using date, if that's not present using creation time as the primary timefield datetime
            if item.get('date'):
                item['datetime'] = parse(item['date']).strftime("%Y-%m-%dT%H:%M:%S.000Z")
            elif item.get('creation time'):
                item['datetime'] = parse(item['creation time']).strftime("%Y-%m-%dT%H:%M:%S.000Z")
                
                    
            #Latin1 encoding works with test data, utf-8 fails.
            if os.path.basename(path) == "Message.html":
                f = open(path,encoding='latin1')
                emailsize += os.path.getsize(path)
                soup = BeautifulSoup(f.read(), 'html.parser')
                item['text'] = soup.get_text()
                f.close()
            
            #Sometimes this exists
            if os.path.basename(path) == "Message.txt":
                f = open(path,encoding='latin1')
                item['text'] = f.read()
                f.close()


    if item is not None:
        
        item['emailsize'] = emailsize
        try:
            if _id:
                
                bulk = [{"_op_type": "index","_index": args.index,"_id":_id,"_source":item}]
                
                result = helpers.bulk(es,bulk)
                
         
                if 'errors' in result and result.get('errors'):
                    print(item)
                    print(result)
                    return False
                
        except Exception as e:
            print(f"{item} ERROR:{e}")
     
            return False
    return True
        

messages = []

           
for f in glob.glob(args.directory+"/**/Message*/",recursive=True):
    messages.append(f)

        
for x in messages:
    process_messages(x)


