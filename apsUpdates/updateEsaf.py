#!/usr/bin/python3

# Core modules
import argparse
import sys
import os
import traceback
import syslog
import subprocess
import types
import time
import base64
import json  # , xml.dom, xml.dom.minidom
import xml.etree.ElementTree as xml
from datetime import datetime
import email
import imaplib
import getpass

# Google's modules
from google.oauth2 import service_account
# from google.oauth2.credentials import Credentials
from googleapiclient import discovery
from googleapiclient import errors

# Our modules
import lslog
import postgresUpdater

description = '''Reads emails containing XML data on ESAFS from the APS 
and updates the ESAF database.

NOTE: If the DEBUG environment variable is set, debug messages will be sent
to stdout.'''
argparser = argparse.ArgumentParser(description=description)

description = '''JSON-based config file. See the provided config.json 
example for a the set of available options.'''
argparser.add_argument('config_file', nargs='?', default='./config.json',
                       help=description)

description = '''If specified, no modifications will be made to the email 
box or the database.'''
argparser.add_argument('--dry-run', action='store_true', help=description)

args = argparser.parse_args()
dryRun = args.dry_run

config = {}
with open(args.config_file, 'r') as fileHandle:
    config = json.load(fileHandle)

gmailConfig = config.get('gmail', {})
gmailUsername    = gmailConfig.get('username', 'esaf@ls-cat.org')
gmailLabels      = gmailConfig.get('labels', ['UNREAD', 'INBOX'])
gmailCredentials = gmailConfig.get('credentials', './credentials/credentials.json')

if os.getuid() != 0:
    lslog.warning('''This script is not running as root, a dry run will be 
performed instead.\n''')
    dryRun = True

#
# Authenticate with the Gmail API under the config file
#


def getGmailService(gmailUsername, credentialsFile):
    # We need the keys to "archive" emails after reading, i.e. strip
    # "UNREAD" and "INBOX" labels.
    accessScopes = ['https://mail.google.com/']

    credentials = service_account.Credentials.from_service_account_file(
        credentialsFile, scopes=accessScopes)
    delegatedCredentials = credentials.with_subject(gmailUsername)

    gmailService = discovery.build('gmail', 'v1',
                                   credentials=delegatedCredentials)
    return gmailService

#
# Returns true if the XML document is a well-formed ESAF doc for our sector.
#
def validateEsafData(dom):
    # Check that sector_no is 21
    tmpRecord = dom.find('sector_no')
    if tmpRecord == None:
        lslog.error('ESAF XML document does not contain sector_no')
        return False
    elif not tmpRecord.text == '21':
        lslog.error('ESAF XML document is not for sector 21, sector_no=%s'
                    % (tmpRecord.text))
        return False

    return True


lslog.info('Searching for ESAF XML emails in account=%s'
           % (gmailUsername))

# TODO: This line causes the following error in pylint:
#   E1101: Instance of 'Resource' has no 'users' member (no-member)
#
# We need to file a ticket with Google, because this worked before.
gmailMessages = getGmailService(gmailUsername,
                                gmailCredentials).users().messages()

gmailQuery = ''
for gmailLabel in gmailLabels:
    gmailQuery = gmailQuery + (' label:%s' % gmailLabel)

results = gmailMessages.list(userId='me', q=gmailQuery).execute()
lslog.info('Found %d ESAF messages in need of processing'
           % (results['resultSizeEstimate']))
if results['resultSizeEstimate'] < 1:
    lslog.info('Nothing to do, exiting.')
    exit()

# Create a storage bin for XML attachments
os.makedirs('./xml', exist_ok=True)

postgresUpdater = postgresUpdater.postgresUpdater(dryRun=dryRun)

# Download the attached XML in each message and iterate from oldest to
# newest email.
def compareFunc(msg):
    return int(msg['internalDate'])

messages = list()
for info in results['messages']:
    msg = gmailMessages.get(userId='me', id=info['id']).execute()
    messages.append(msg)
messages.sort(key=compareFunc)

for msg in messages:
    info = {'Gmail Message ID': msg['id'],
            'Date': str(datetime.fromtimestamp(int(msg['internalDate'])/1000))}
    lslog.info('Processing message - %s' % (str(info)))

    # The email is most likely multipart, so we must iterate through
    # each part until we find an attachment id.
    attachmentId = msg['payload']['body'].get("attachmentId")
    filename = msg['payload'].get('filename')
    for part in msg['payload'].get('parts', []):
        if attachmentId:
            print(filename)
            break

        attachmentId = part['body'].get('attachmentId')
        filename = part.get('filename')

    if not attachmentId:
        lslog.warning('Not a valid ESAF email (no attachment), skipping')
        continue
    elif not (filename.startswith('esaf_xml_attach_') and
              filename.endswith('.xml')):
        lslog.warning(
            'Not a valid ESAF email (unrecognized attachment), skipping')
        continue

    attachment = gmailMessages.attachments().get(userId='me',
                                                 messageId=msg['id'],
                                                 id=attachmentId).execute()

    # From inception, the ESAF email system has been using ISO-8859-1, but
    # when the APS gets with the times and uses UTF-8, this check will
    # prevent future disruption.
    # -Jory Folker, 7/2023
    decodedData = base64.urlsafe_b64decode(attachment['data'])
    if decodedData.find(b'<?xml version="1.0" encoding="ISO-8859-1"?>') >= 0:
        decodedData = decodedData.decode('iso-8859-1')
    else:
        decodedData = decodedData.decode('utf-8')

    # If the document is malformed or doesn't belong to our sector, keep the
    # original email around, store a local copy of the XML, and let the
    # administrator know.
    doc = xml.fromstring(decodedData)
    xmlFileDest = './xml/' + filename
    if doc is None or not validateEsafData(doc):
        lslog.info('skipping email that does not contain an ESAF XML update')
        continue

    # Always write a copy of the XML file for inspection.
    if not os.path.exists(xmlFileDest):
        f = open(xmlFileDest, 'w')
        f.write(decodedData)
        f.close()
        lslog.debug('saved a copy of %s to %s' % (xmlFileDest, os.getcwd()))

    todo = None
    try:
        todo = postgresUpdater.processDocument(doc)
    except Exception as e:
        lslog.error('failed to update postgres for an ESAF, the gmail message is left unread. ', e)
        continue

    p = None
    try:
        for esafno, people in todo.items():
            lslog.info(f'lscat-addesaf {esafno}')
            p = subprocess.run(['/usr/local/sbin/lscat-addesaf', str(esafno)], capture_output=True)
            p.check_returncode()

            for badgeno, userinfo in people.items():
                lslog.info(f'lscat-adduser {badgeno} {userinfo[0]} {userinfo[1]} {userinfo[2]}')
                p = subprocess.run(['/usr/local/sbin/lscat-adduser', str(badgeno), userinfo[0], userinfo[1], userinfo[2] ], capture_output=True)
                p.check_returncode()

                lslog.info(f'lscat-addaccess {badgeno} {esafno}')
                p = subprocess.run(['/usr/local/sbin/lscat-addaccess', str(badgeno), str(esafno)], capture_output=True)
                p.check_returncode()

    except subprocess.CalledProcessError as e:
        lslog.error(f"{p.args[0]} failed with status {p.returncode}: {p.stderr.decode().replace('\n', '\\n')}")
        continue

    if dryRun:
        lslog.notice('dry run ESAF update successful\n')
    else:
        requestBody = {"removeLabelIds": gmailLabels}
        gmailMessages.modify(userId='me', id=msg['id'],
                             body=requestBody).execute()
        lslog.notice('ESAF update successful, gmail message moved to archive')
