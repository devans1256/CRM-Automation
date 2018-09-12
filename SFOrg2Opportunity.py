# -*- coding: utf-8 -*-
"""
Created on SAT AUG 25 10:43:18 2018

@author: UNair
"""
# --- Imports ---
import sys
import os
import csv
import gc
import shutil
import pandas as pd
import datetime as MyDT
from datetime import datetime
import smtplib
from simple_salesforce import Salesforce
# --- Set up Static variables ---
my_email_server = "mail.avidxchange.net"
my_email_to = ['VCCAnalysts@avidxchange.com', 'dataintegrityops@avidxchange.com', 'devans@avidxchange.com']
my_email_to_err = ['VCCAnalysts@avidxchange.com', 'dataintegrityops@avidxchange.com', 'devans@avidxchange.com']
my_email_from = "SFORG2-Opportunity@avidxchange.com"
my_email_port = 25
#
my_sf_user = 'devansorg2@avidxchange.com'
my_sf_pass = 'Addy%11142016'

my_sf_token = 'diL11Vy07H53psQW9gC0dxhHX'
my_sf_instance = "na88.salesforce.com"
#
my_pgm_log = "e:/Archive/Opportunity/pgm_log.txt"
file_desc = 'Opportunity Data'
dir_in = 'e:/Opportunity-dropbox/'
dir_out = "e:/Archive/Opportunity/"
# --- Build log file ---
log_file = open(my_pgm_log,"a")
d=datetime.now()
d_str = str(d)
my_log_msg = (d_str + " Opportunity Program started: " +  "\n")
log_file.write(my_log_msg)
my_flag =" "

items_updated = 0
items_failed = 0
tot_items = 0
# -- my_err_list is used to create error lines in email ---
my_err_list = " "

# Process a Fatal error
def fatal_err(error_msg):
    d=datetime.now()
    d_str=str(d)
    log_file.write(d_str + " " + error_msg + "\n")
    log_file.write(d_str + " ** Program Terminated - Failure ** " + "\n")
    log_file.close()
    sys.exit()
# Email Functions
def send_pass_email(str_items_updated, str_tot_items, str_items_failed,email_file_processed):
    global my_email_pass, my_email_user,my_email_server, my_email_to, my_email_from       
    server = smtplib.SMTP(my_email_server, my_email_port)   
 #   my_email_to = ['devans@avidxchange.com']
    my_subj = "Successful - Opportunity Run Confirmation for: " + email_file_processed
    my_body = 'Number of Opportunity successfully Processed: ' + str_items_updated + " \n" + "  Opportunity Items Failed: " + str_items_failed + " \n" + "  Total Opportunity Processed: " + str_tot_items
    my_email_body = 'Subject: {}\n\n{}'.format(my_subj, my_body)
    server.sendmail(my_email_from, my_email_to, my_email_body)
    server.quit()
#    
def send_err_email(str_items_failed,email_file_processed, str_items_updated, str_tot_items, my_err_list):   
    global my_email_pass, my_email_user,my_email_server, my_email_to, my_email_from  
    server = smtplib.SMTP(my_email_server, my_email_port)
#    my_email_to = "SFDCOrg2Team@avidxchange.com"
#    my_email_to_err = ['DEvans@avidxchange.com']
    my_subj = "Failed - Opportunity for:  " + email_file_processed + " !! Please check Log file for errors !!"
    my_body = 'Message:  ' + " \n" + ' --- Number of Opportunity successfully Processed: ' + str_items_updated + " \n" + "  Opportunity Items Failed: " + str_items_failed + " \n" + "  Total Opportunity Processed: " + str_tot_items + " \n  " + my_err_list
    my_email_body = 'Subject: {}\n\n{}'.format(my_subj, my_body)
    server.sendmail(my_email_from, my_email_to_err, my_email_body)
    server.quit()
# sign on to Sales Force
try:
    sf = Salesforce(username= my_sf_user,
                password= my_sf_pass, 
                security_token=my_sf_token,
                instance_url = my_sf_instance)
except Exception as e:
    error_msg=str(e)
    error_msg = error_msg 
    send_err_email("1")
    fatal_err(error_msg)
    
    

# Check if there is a file to process
if len(os.listdir(dir_in) ) == 0:
    my_log_msg = (d_str + " *** No Files Found to Process ** " +  "\n")
    log_file.write(my_log_msg)
    log_file.close()
    sys.exit("** No Files Found to Process **")

# Get list of files to Process
my_in_dir=os.listdir(dir_in)
# Process each file in directory
for item in my_in_dir:
    if item.endswith(".xlsx"):
#        file_2_process = dir_in + item
        file_2_convert = dir_in + item
        data_xls = pd.read_excel(file_2_convert, 'Sheet1', index_col=None)
        file_2_process = dir_in + "opportunity.csv"
        data_xls.to_csv(file_2_process, date_format='%m/%d/%Y', index=False)
        
        
        
        email_file_processed = file_2_process
# lets get the date and time stamp to qualify archives
        file_Archive = MyDT.datetime.now().strftime("%Y%m%d-%H%M%S")

# set file names
#        my_file_in = (file_2_process)
        my_file_out_archive =(dir_out + file_Archive + ".csv")

#copy the incoming file to archive folder
        shutil.copy(file_2_process, my_file_out_archive) 

# --- Main Process Read Data in and Push to SalesForce      
        f = open(file_2_process)
        # --> Reset all Counters  
        items_updated = 0
        items_failed = 0
        tot_items = 0
    
        # --> Read the CSV file
        reader = csv.DictReader(f)
        for row in reader:
            tot_items += 1
#        # --> Extract Data from CSV    
            inDate = datetime.strptime((row["Invoice Date"]), "%m/%d/%Y")
            sfDate = datetime.strftime(inDate, "%Y-%m-%d") 
            ext_id = (row["Vendor GUID"])
                                                         
#     # --> Build Query String       
            qry_str = ("Select Id From Address_Profile__c Where Address_Profile_VMS_GUID__c = " + "'" + ext_id + "'")
#        # --> find out if reocrds were returned
            records = sf.query(qry_str)
#            print(records)
            nbr_found = (records['totalSize']) 
            if nbr_found == 0:
                my_flag = "New"
                cust_key = 'VMS_GUID__c/' + ext_id
            else:
                my_flag = "Update"
                records = records['records']
                for record in records:
                    cust_key = (record['Id'])
#                      
            sf_organization_name =(row["Organization Name"])
            sf_invoice_description = (row["Description"])
            sf_payer_name = (row["Payer Name"])
            sf_invoice_no = (row["Invoice No."])
            sf_invoice_date = sfDate
            sf_pay_amt = (row["Total Amount"])
         # --> Ok now cleanse the data 
            sf_organization_name = sf_organization_name[:120]
            sf_invoice_description = sf_invoice_description[:255]
            sf_payer_name = sf_payer_name[:120]
            sf_invoice_no = sf_invoice_no[:80]  
                             
         # --> Upset Salesforce Opportunity             
            try:
                       
                sf.Address_profile__c.upsert(cust_key, {
                                 'Organization_Name__c': sf_organization_name,
                                 'Invoice_Description__c':sf_invoice_description,
                                 'Payer_Name__c':sf_payer_name,
                                 'Invoice_Date__c':sfDate,
                                 'Invoice_Number__c':sf_invoice_no,
                                 'Payment_Amount__c':sf_pay_amt,               
                                                   })
                                   
                items_updated +=1
            except Exception as e:
            # --> Error has Occurred    
                        error_msg=str(e)
                        items_failed +=1
            # email error list messages (stacks all errors in email)
                        my_err_list = my_err_list + " \n" + " Error Message: " + cust_key + " " + error_msg + " \n"                        
                        my_log_msg = (d_str + " !!! Error: Address_Profile_VMSGUID = " + ext_id + " Error Message: " + error_msg + "\n")
                        log_file.write(my_log_msg)       
    
    # -->  Finish for current File
        f.close() 
        os.remove(file_2_process)
        os.remove(file_2_convert)
        # --> if no errors send "Success" email to teams        
        if items_failed == 0:
            str_items_updated = str(items_updated)
            str_tot_items = str(tot_items)
            str_items_failed = str(items_failed)
            send_pass_email(str_items_updated, str_tot_items, str_items_failed,email_file_processed)
            items_updated = 0
            items_failed = 0
            tot_items = 0
        else:
        # --> else there was an error send "Failure" email to team
            str_items_failed= str(items_failed)
            str_items_updated = str(items_updated)
            str_tot_items = str(tot_items)
            send_err_email(str_items_failed, email_file_processed, str_items_updated, str_tot_items, my_err_list)
            items_updated = 0
            items_failed = 0            
            tot_items = 0
# All files processed = write log - Close - Clear Memory
d=datetime.now()
d_str = str(d)
log_file.write(d_str + " ** Program Completed **" + "\n") 
log_file.close()
gc.collect()