
# -*- coding: utf-8 -*-
"""
Created 7/5/2018

@author: DEvans
"""

# --- Import Libraries
import sys
import os
import csv
import gc
import shutil
import smtplib
from datetime import datetime
from simple_salesforce import Salesforce

# --- Set Global Variables ---
# -- SMTP Variables
my_email_server = "mail.avidxchange.net"
my_email_to = ["Treasury@avidxchange.com", 'SFDCOrg2Team@avidxchange.com', 'devans@avidxchange.com']
#my_email_to_err = ['devans@avidxchange.com']
# my_email_to = ['devans@avidxchange.com']
my_email_to_err = ['SFDCOrg2Team@avidxchange.com', 'devans@avidxchange.com']
my_email_to_err = ['devans@avidxchange.com']
my_email_from = "SFOrg2-Treasury@avidxchange.com"
my_email_port = 25
# -- SalesForce Org Variables
my_sf_user = 'devansorg2@avidxchange.com'
my_sf_pass = 'Addy%11142016'
my_sf_token = 'diL11Vy07H53psQW9gC0dxhHX'
my_sf_instance = "na88.salesforce.com"

my_pgm_log = "E:/Archive/treasury/pgm_log.txt"
file_desc = "Treasury Data"
dir_in = "e:/Treasury-Dropbox/"
dir_out = "E:/Archive/treasury/"
dir_out_pass = "E:/Archive/treasury/passed/"
dir_out_fail = "E:/Archive/treasury/failed/"
log_file = open(my_pgm_log,"a")
d=datetime.now()
d_str = str(d)
log_file.write(d_str + " Treasury Batch Program started: " +  "\n" )
my_flag =" "

items_updated = 0
items_failed = 0
tot_items = 0
my_err_list = ""

# --- Check for valid date ---  
def check_dates(my_date, error_flag):
    error_flag = 'N'
          
    try:
        datetime.strptime(my_date, "%m/%d/%Y")
        
    except ValueError:
    
        error_flag = 'Y'
    return(my_date, error_flag)
# --- Check that ID exist ---
def check_id(my_guid, error_flag):
    error_flag = "N"
    my_test = str(my_guid)
    if my_test == "":
        error_flag = 'Y'
    return(my_guid, error_flag)

 # -- Email Routines ----   
def send_pass_email(str_items_updated, file_desc, str_file_errors, str_total_items):
    global my_email_pass, my_email_user,my_email_server, my_email_to, my_email_from
    
    
    server = smtplib.SMTP(my_email_server, my_email_port)
   
    
    my_subj = "SUCCESS: " + file_desc
    my_body = 'Number of records successfully Processed: ' + str_items_updated  + " \n" + "  Treasury Items Failed: " + str_file_errors + " \n" + "  Total Treasury Items Processed: " + str_total_items
    my_email_body = 'Subject: {}\n\n{}'.format(my_subj, my_body)
    server.sendmail(my_email_from, my_email_to, my_email_body)
    server.quit()

    
def send_err_email(file_desc, my_failed_file, total_items, file_errors, my_err_list):
    str_tot_items = str(total_items)
    str_file_err = str(file_errors)
    global my_email_pass, my_email_user,my_email_server, my_email_to, my_email_from  
    server = smtplib.SMTP(my_email_server, my_email_port)
    
    my_subj = "FAILED: " + file_desc
    my_body = 'Error file is located: ' + my_failed_file + " --- Total Items Processed: " + str_tot_items + " Total Failed: " + str_file_err + my_err_list
    my_email_body = 'Subject: {}\n\n{}'.format(my_subj, my_body)
    server.sendmail(my_email_from, my_email_to_err, my_email_body)
    server.quit()
    
    
# Process a Fatal error
def fatal_err(error_msg):
    d=datetime.now()
    d_str=str(d)
    log_file.write(d_str + " " + error_msg + "\n")
    log_file.write(d_str + " ** Program Terminated - Failure ** " + "\n")
    log_file.close()
    sys.exit()
    
def close_my_files():
    f.close()
    pf.close()
    ff.close()
    
try:
    sf = Salesforce(username= my_sf_user,
                password= my_sf_pass, 
                security_token= my_sf_token,
                instance_url = my_sf_instance,
                )
except Exception as e:
    file_desc ='SalesForce Error:'
    error_msg=str(e)
    error_msg = error_msg 
    send_err_email(file_desc, error_msg, "0", "0")
    fatal_err(error_msg)

# --- Check if there is a file to process
if len(os.listdir(dir_in) ) == 0:
    d=datetime.now()
    d_str = str(d)
    log_file.write(d_str + " ** No Files Found ** " +  "\n" )
    log_file.write(d_str + " Program has Terminated" +  "\n" )
    sys.exit()
    
total_items = 0
file_errors = 0
items_updated = 0

my_In_Dir=os.listdir(dir_in)
# --- Loop through directory for files to process
for item in my_In_Dir:
    if item.endswith(".csv"):
        # --- Lets build the path for files
        my_file_name = item
        my_pass_fail = "SUCCESS "
        d=datetime.now()
        my_mess_date = d.strftime("%m/%d/%y %H:%M %p")
        
        
        my_date = (d.strftime("%Y%m%d%H%M%S"))
        sf_date = my_date
        file_2_process =(dir_in + my_file_name)
#       my_file_archive = (dir_out + my_date + my_file_name)
        my_file_archive = (dir_out + my_file_name)
        my_pass_file = (dir_out_pass + my_date + my_file_name)
        my_failed_file = (dir_out_fail + my_date + my_file_name)
        # --- Copy files to archive before processing
        shutil.copy(file_2_process, my_file_archive)
        file_2_process_lc = file_2_process.lower()

        
            
        # --- Else If Checks Cleared Update (Old) file - process
        if 'checks cleared update (old)' in file_2_process_lc:
            file_errors = 0
            total_items = 0
            items_updated = 0
            my_err_list = " "
            error_flag = 'N'
            file_desc = "AM - Checks Cleared Update (Old): " + my_mess_date 
            f = open(file_2_process)
            ff = open(my_failed_file,'w',newline='')
            ff_write = csv.writer(ff)
            ff_write.writerow(("Payment ID (Salesforce)", "Cleared Date", "PaymentID", "Status", "Reason"))
            
            pf = open(my_pass_file,'w', newline='')
            pf_write = csv.writer(pf)
            pf_write.writerow(("Payment ID (Salesforce)", "Cleared Date", "PaymentID", "Status"))
            reader = csv.DictReader(f)
            for row in reader:
                total_items += 1
                my_date = (row["Cleared Date"])
                my_payid = (row["PaymentID"])
                my_guid = (row["Payment ID (Salesforce)"])
                (my_date, error_flag) =check_dates(my_date, error_flag)  
                try:               
                    if error_flag == 'Y':
                        file_errors += 1
                        ff_write.writerow((my_guid,my_date,my_payid, "Failed", "Bad Date"))
                    else:
                        in_date = datetime.strptime((row["Cleared Date"]), "%m/%d/%Y")
                        sf_date = datetime.strftime(in_date, "%Y-%m-%d")           
                        my_guid = (row["Payment ID (Salesforce)"]) 
                        (my_guid, error_flag) = check_id(my_guid, error_flag)
                    
                        if error_flag == 'N':                        
                            my_guid = (row["Payment ID (Salesforce)"])
                            sf.Payment__c.upsert(my_guid,
                                             {'Payment_Cleared_Date__c' : sf_date 
                                             }) 
                            pf_write.writerow((my_guid,my_date,my_payid, "Upserted"))
                            items_updated += 1                                           
                        else:
                            file_errors += 1
                            my_err_list = my_err_list + " \n" + " Error Message: bad Id " + my_guid + " \n"
                            ff_write.writerow((my_guid,my_date,my_payid, "Failed", "Bad ID"))
                except Exception as e:                   
                            error_msg=str(e)
                            error_msg = error_msg
                            file_errors += 1
                            my_err_list = my_err_list + " \n" + " Error Message: " + error_msg + " \n"
                            ff_write.writerow((my_guid,my_date,my_payid, "Failed", error_msg))
            # --- Close and Remove File --- 
            close_my_files()
      #      f.close()
      #      pf.close()
      #      ff.close()
            
            if file_errors > 0:
                send_err_email(file_desc, my_failed_file,total_items, file_errors, my_err_list)
            else:
                str_items_updated = str(items_updated)
                str_file_errors = str(file_errors)
                str_total_items = str(total_items)
                send_pass_email(str_items_updated, file_desc, str_file_errors, str_total_items)
                os.remove(my_failed_file)
                      
            os.remove(file_2_process)
            
        # --- Else If ONST Approval Update New file - process
        
            
        # --- Else If ONST Approval Update Old file - process
        elif 'onst approval update (old)' in file_2_process_lc:
            file_errors = 0
            total_items = 0
            items_updated = 0
            error_flag = 'N'
            my_err_list = " "
            file_desc = "PM - ONST Approval Update (Old): " + my_mess_date
            f = open(file_2_process)
            ff = open(my_failed_file,'w',newline='')
            ff_write = csv.writer(ff)
            ff_write.writerow(("Payment ID (Salesforce Record ID)", "ONST Approved Date", "ONST Check #", "Rejected Internal ID","Check Date","SunGard Job ID","PaymentID", "Status", "Reason"))
            
            pf = open(my_pass_file,'w', newline='')
            pf_write = csv.writer(pf)
            pf_write.writerow(("Payment ID (Salesforce Record ID)", "ONST Approved Date", "ONST Check #","Rejected Internal ID","Check Date","SunGard Job ID", "PaymentID", "Status"))
            reader = csv.DictReader(f)
            for row in reader:
                total_items += 1
                my_chk_date = (row["Check Date"])
                my_app_date = (row["ONST Approved Date"])
                my_guid = (row["Payment ID (Salesforce Record ID)"])
                my_ONST_check = (row["ONST Check #"])
                my_rejected_id = (row["Rejected Internal ID"]) 
                my_sungard_id = (row["SunGard Job ID"])
                my_payid = (row["PaymentID"])
                my_dates_chk = "N"
                my_date = my_chk_date
                (my_date, error_flag) =check_dates(my_date, error_flag)
                
                if error_flag == "Y":
                    my_dates_chk = "Y"
        # --- Need to validate Approved Date ----
                my_date = my_app_date
                (my_date, error_flag) =check_dates(my_date, error_flag)
                try:
                    if error_flag == "Y":
                        my_dates_chk = "Y"
                    
                    if error_flag == 'Y':
                        file_errors += 1
                        ff_write.writerow((my_guid,my_app_date,my_ONST_check,my_rejected_id,my_chk_date,my_sungard_id,my_payid, "Failed", "Bad Date"))
                    else:
                            in_date = datetime.strptime((row["ONST Approved Date"]), "%m/%d/%Y")
                            sf_date2 = datetime.strftime(in_date, "%Y-%m-%d")            
               
                            in_date = datetime.strptime((row["Check Date"]), "%m/%d/%Y")
                            sf_date = datetime.strftime(in_date, "%Y-%m-%d")
                            (my_guid, error_flag) = check_id(my_guid, error_flag)
                            rejected_nf = "Y"
                            sangard_nf = "Y"
                            if my_rejected_id.split():
                                rejected_nf = "N"
                            if my_sungard_id.split():
                                sungard_nf = "N"
                    
                            if error_flag == 'N':                      
                                my_guid = (row["Payment ID (Salesforce Record ID)"])  
                                if rejected_nf == "N" and sangard_nf == "N":
                                    sf.Payment__c.update(my_guid,
                                                         {'Payment_ONST_Approved_Date__c' : sf_date2,
                                                          'Payment_ONST_Check__c' : my_ONST_check,
                                                          'Payment_Check_Date__c' : sf_date,
                                                          'Payment_Rejected_Internal_ID__c' : my_rejected_id,
                                                          'Payment_SunGard_Job_ID__c' : my_sungard_id,                  
                                                          })
                                else:
                                    if rejected_nf == "Y" and sungard_nf == "N":
                                        sf.Payment__c.update(my_guid,
                                                         {'Payment_ONST_Approved_Date__c' : sf_date2,
                                                          'Payment_ONST_Check__c' : my_ONST_check,
                                                          'Payment_Check_Date__c' : sf_date,                                                        
                                                          'Payment_SunGard_Job_ID__c' : my_sungard_id,                  
                                                          })
                                    else:
                                        if rejected_nf == "N" and sungard_nf == "Y":
                                            sf.Payment__c.update(my_guid,
                                                         {'Payment_ONST_Approved_Date__c' : sf_date2,
                                                          'Payment_ONST_Check__c' : my_ONST_check,
                                                          'Payment_Check_Date__c' : sf_date,                                                        
                                                          'Payment_SunGard_Job_ID__c' : my_rejected_id,                  
                                                          })
                              # ---- both rejected and sangard are not found ---
                                        else:
                                            sf.Payment__c.update(my_guid,
                                                         {'Payment_ONST_Approved_Date__c' : sf_date2,
                                                          'Payment_ONST_Check__c' : my_ONST_check,
                                                          'Payment_Check_Date__c' : sf_date,                                                        
                                                                            
                                                          })
                                        
                                pf_write.writerow((my_guid,my_app_date,my_ONST_check,my_rejected_id,my_chk_date,my_sungard_id,my_payid, "Updated"))
                                items_updated += 1                                        
                            else:
                                file_errors += 1
                                my_err_list = my_err_list + " \n" + " Error Message: Bad Id " + my_guid + " \n"
                                ff_write.writerow((my_guid,my_app_date,my_ONST_check,my_rejected_id,my_chk_date,my_sungard_id,my_payid, "Failed", "Bad ID"))
                except Exception as e:                          
                            error_msg=str(e)
                            error_msg = error_msg
                            file_errors += 1
                            my_err_list = my_err_list + " \n" + " Error Message: " + error_msg + " \n"
                            ff_write.writerow((my_guid,my_date,my_ONST_check,my_rejected_id,my_chk_date,my_sungard_id,my_payid, "Failed", error_msg))
            # --- Close and Remove File  --- 
            close_my_files()
   #         f.close()
   #         pf.close()
   #         ff.close()
            
            if file_errors > 0:
                send_err_email(file_desc, my_failed_file,total_items, file_errors, my_err_list)
            else:
                str_items_updated = str(items_updated)
                str_file_errors = str(file_errors)
                str_total_items = str(total_items)
                send_pass_email(str_items_updated, file_desc, str_file_errors, str_total_items)
                os.remove(my_failed_file)
                      
            os.remove(file_2_process)
            
    
        else:
            # --- Error -> Could not make out name of file to process ---
            #file_desc = "** Batch file not found **"
            d=datetime.now()
            d_str = str(d)
            log_file.write(d_str + " No Batch Files to process ** " +  "\n")
            #my_failed_file = "Don't Understand: " + file_2_process
            #send_err_email(file_desc, my_failed_file,total_items, file_errors)
        
    else:
        # --- Error -> File is not a CSV format ----
        file_desc = "** Bad File Extension **"
        my_failed_file = "CSV file expected: " + file_2_process
        send_err_email(file_desc, my_failed_file,total_items, file_errors)
    
d=datetime.now()
d_str = str(d)
log_file.write(d_str + " Treasury Batch Program Ended: " +  "\n")
log_file.close()
# --- Garbage Collection ---
gc.collect()

