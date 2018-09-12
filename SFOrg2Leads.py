# -*- coding: utf-8 -*-
"""
Created on Mon Jun 18 10:43:18 2018

@author: DEvans
"""
# --- Imports ---
import sys
import os
import csv
import gc
import shutil
import datetime as MyDT
from datetime import datetime
import smtplib
from simple_salesforce import Salesforce
# --- Set up Static variables ---

my_email_server = "mail.avidxchange.net"
my_email_to = ['VCCAnalysts@avidxchange.com', 'dataintegrityops@avidxchange.com', 'devans@avidxchange.com']
my_email_from = "SFORG2-Leads@avidxchange.com"
my_email_port = 25
my_sf_user = 'devansorg2@avidxchange.com'
my_sf_pass = 'Addy%11142016'

my_sf_token = 'diL11Vy07H53psQW9gC0dxhHX'
my_sf_instance = "na88.salesforce.com"
my_pgm_log = "e:/Archive/leads/pgm_log.txt"
file_desc = 'Leads Data'
dir_in = 'e:/Leads-dropbox/'
dir_out = "e:/Archive/leads/"
# --- Build log file ---
log_file = open(my_pgm_log,"a")
d=datetime.now()
d_str = str(d)
my_log_msg = (d_str + " Leads Program started: " +  "\n")
log_file.write(my_log_msg)
my_flag =" "

items_updated = 0
items_failed = 0
tot_items = 0
my_err_list =""

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
    my_email_to = ['VCCAnalysts@avidxchange.com', 'DataIntegrityOps@avidxchange.com','SFDCOrg2Team@avidxchange.com', 'devans@avidxchange.com']
    my_subj = "Successful - Leads Run Confirmation for: " + email_file_processed
    my_body = 'Number of Leads successfully Processed: ' + str_items_updated + " \n" + "  Lead Items Failed: " + str_items_failed + " \n" + "  Total Leads Processed: " + str_tot_items
    my_email_body = 'Subject: {}\n\n{}'.format(my_subj, my_body)
    server.sendmail(my_email_from, my_email_to, my_email_body)
    server.quit()
#    
def send_err_email(str_items_failed,email_file_processed, str_items_updated, str_tot_items, my_err_list):
    
    global my_email_pass, my_email_user,my_email_server, my_email_to, my_email_from  
    server = smtplib.SMTP(my_email_server, my_email_port)
#    my_email_to = "SFDCOrg2Team@avidxchange.com"
    my_email_to_err = ['DEvans@avidxchange.com', 'SFDCOrg2Team@avidxchange.com', 'VCCAnalysts@avidxchange.com', 'DataIntegrityOps@avidxchange.com']
    my_subj = "Failed - Leads for:  " + email_file_processed + " !! Please check Log file for errors !!"
    my_body = " \n" + ' --- Number of Leads successfully Processed: ' + str_items_updated + " \n" + "  Lead Items Failed: " + str_items_failed + " \n" + "  Total Leads Processed: " + str_tot_items + my_err_list
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
    if item.endswith(".csv"):
        file_2_process = dir_in + item
        email_file_processed = item
# lets get the date and time stamp to qualify archives
        file_Archive = MyDT.datetime.now().strftime("%Y%m%d-%H%M%S")

# set file names
        my_file_in = (file_2_process)
        my_file_out_archive =(dir_out + file_Archive + ".csv")

#copy the incoming file to archive folder
        shutil.copy(my_file_in, my_file_out_archive) 

# --- Main Process Read Data in and Push to SalesForce      
        f = open(my_file_in)
        # --> Reset all Counters  
        items_updated = 0
        items_failed = 0
        tot_items = 0
        my_err_list = " "
    
        # --> Read the CSV file
        reader = csv.DictReader(f)
        for row in reader:
            tot_items += 1
        # --> Extract Data from CSV    
            inDate = datetime.strptime((row["JIT Invoice Date"]), "%m/%d/%Y")
            sfDate = datetime.strftime(inDate, "%Y-%m-%d") 
            ext_id = (row["VMS GUID"])
     # --> Build Query String       
            qry_str = ("Select SF_Lead_ID__c From Lead Where VMS_GUID__c = "+ "'" + ext_id + "'")
        # --> find out if reocrds were returned
            records = sf.query(qry_str)
            nbr_found = (records['totalSize']) 
            
           
            if nbr_found == 0:
                my_flag = "New"
                cust_key = 'VMS_GUID__c/' + ext_id
            else:
                my_flag = "Update"
                records = records['records']
                for record in records:
                    cust_key = (record['SF_Lead_ID__c'])
            
                
          # --  print(my_flag)
          # --   print(cust_key)
            my_status_chk = (row["Status"])
         # --   print(my_status_chk)
            sf_company =(row["Company"])
            sf_lname = (row["Last Name"])
            sf_contracting = (row["Contacting on Behalf Of"])
            sf_jit_nbr = (row["JIT Invoice Number"])
            sf_jit_payd = (row["JIT Payment Description"])
            sf_svc_loc = (row["Service Location Address"])
            sf_vmi_city = (row["VMI City"])
            sf_vmi_country = (row["VMI Country"])
            sf_vmi_state = (row["VMI State"])
            sf_vmi_street = (row["VMI Street"])
            sf_vmi_zip = (row["VMI Zip Code"])
            sf_pend_pay = (row["# of Pending Payments"])
            sf_campaign = (row["Campaign Type"])
            sf_jit_pay_s = (row["JIT Payment Spend $$$"])
            sf_orig_client = (row["Original Client Relationship"])
            sf_vmi_phone = (row["VMI Phone"])
         # --> Ok now cleanse the data 
            sf_company = sf_company[:255]
            sf_lname = sf_lname[:80]
            sf_contracting = sf_contracting[:255]
            sf_jit_nbr = sf_jit_nbr[:30]
            sf_jit_payd = sf_jit_payd[:150]
            sf_svc_loc = sf_svc_loc[:255]
            sf_vmi_city = sf_vmi_city[:80]
            sf_vmi_country = sf_vmi_country[:80]
            sf_vmi_state = sf_vmi_state[:80]
            sf_vmi_street = sf_vmi_street[:100]
            sf_vmi_zip = sf_vmi_zip[:20]
            
         # --> Update/Insert Salesforce lead             
            try:
                if my_status_chk.strip():
                    # status found
                  # --  print(my_status_chk)
                    if my_flag == "New":
                        sf.Lead.upsert(cust_key, {
                                'Company': sf_company,        
                                'LastName': sf_lname,        
                                'Status': my_status_chk,
                                'of_Pending_Payments__c': sf_pend_pay,
                                'Campaign_Type__c': sf_campaign,
                                #'VMS_GUID__c':(row["VMS GUID"]),
                                'Do_you_know_any_AvidXchange_Clients__c': sf_contracting,
                                'JIT_Invoice_Date__c': sfDate,
                                'JIT_Invoice_Number__c': sf_jit_nbr,
                                'JIT_Payment_Description__c': sf_jit_payd,
                                'JIT_Payment_Spend__c': sf_jit_pay_s,
                                'Original_Client_Relationship__c': sf_orig_client,
                                'Service_Location_Address__c': sf_svc_loc,
                                'VMI_City__c': sf_vmi_city,
                                'VMI_Country__c': sf_vmi_country,
                                'VMI_Phone__c': sf_vmi_phone,
                                'VMI_State__c': sf_vmi_state,
                                'VMI_Street__c': sf_vmi_street,
                                'VMI_Zip_Code__c': sf_vmi_zip                       
                                                   })
                    else:
                        sf.Lead.update(cust_key, {
                                'Company': sf_company,        
                                'LastName': sf_lname,        
                                'Status': my_status_chk,
                                'of_Pending_Payments__c': sf_pend_pay,
                                'Campaign_Type__c': sf_campaign,
                                # 'VMS_GUID__c':(row["VMS GUID"]),
                                'Do_you_know_any_AvidXchange_Clients__c': sf_contracting,
                                'JIT_Invoice_Date__c': sfDate,
                                'JIT_Invoice_Number__c': sf_jit_nbr,
                                'JIT_Payment_Description__c': sf_jit_payd,
                                'JIT_Payment_Spend__c': sf_jit_pay_s,
                                'Original_Client_Relationship__c': sf_orig_client,
                                'Service_Location_Address__c': sf_svc_loc,
                                'VMI_City__c': sf_vmi_city,
                                'VMI_Country__c': sf_vmi_country,
                                'VMI_Phone__c': sf_vmi_phone,
                                'VMI_State__c': sf_vmi_state,
                                'VMI_Street__c': sf_vmi_street,
                                'VMI_Zip_Code__c': sf_vmi_zip                       
                                                   }, headers={'Sforce-Auto-Assign': 'FALSE'})
                else:
                  # --   print("status  is blank")
                    if my_flag == "New":
                        sf.Lead.upsert(cust_key, {
                                'Company': sf_company,        
                                'LastName': sf_lname,        
                                'of_Pending_Payments__c': sf_pend_pay,
                                'Campaign_Type__c': sf_campaign,
                               # 'VMS_GUID__c':(row["VMS GUID"]),
                                'Do_you_know_any_AvidXchange_Clients__c': sf_contracting,
                                'JIT_Invoice_Date__c': sfDate,
                                'JIT_Invoice_Number__c': sf_jit_nbr,
                                'JIT_Payment_Description__c': sf_jit_payd,
                                'JIT_Payment_Spend__c': sf_jit_pay_s,
                                'Original_Client_Relationship__c': sf_orig_client,
                                'Service_Location_Address__c': sf_svc_loc,
                                'VMI_City__c': sf_vmi_city,
                                'VMI_Country__c': sf_vmi_country,
                                'VMI_Phone__c': sf_vmi_phone,
                                'VMI_State__c': sf_vmi_state,
                                'VMI_Street__c': sf_vmi_street,
                                'VMI_Zip_Code__c': sf_vmi_zip                       
                                                   })
                    else:
                        sf.Lead.update(cust_key, {
                                'Company': sf_company,        
                                'LastName': sf_lname,        
                                #'Status': my_status_chk,
                                'of_Pending_Payments__c': sf_pend_pay,
                                'Campaign_Type__c': sf_campaign,
                                # 'VMS_GUID__c':(row["VMS GUID"]),
                                'Do_you_know_any_AvidXchange_Clients__c': sf_contracting,
                                'JIT_Invoice_Date__c': sfDate,
                                'JIT_Invoice_Number__c': sf_jit_nbr,
                                'JIT_Payment_Description__c': sf_jit_payd,
                                'JIT_Payment_Spend__c': sf_jit_pay_s,
                                'Original_Client_Relationship__c': sf_orig_client,
                                'Service_Location_Address__c': sf_svc_loc,
                                'VMI_City__c': sf_vmi_city,
                                'VMI_Country__c': sf_vmi_country,
                                'VMI_Phone__c': sf_vmi_phone,
                                'VMI_State__c': sf_vmi_state,
                                'VMI_Street__c': sf_vmi_street,
                                'VMI_Zip_Code__c': sf_vmi_zip                       
                                                   }, headers={'Sforce-Auto-Assign': 'FALSE'})
                    
                items_updated +=1
            except Exception as e:
            # --> Error has Occurred    
                error_msg=str(e)
                items_failed +=1
                my_err_list = my_err_list + " \n" + " Error: Guid = " + cust_key + "  Company: " + sf_company + "  Message: " + error_msg  + " \n"
                my_log_msg = (d_str + " !!! Error: VMSGUID = " + ext_id + " Error Message: " + error_msg + "\n")
                log_file.write(my_log_msg)       
    
    # -->  Finish for current File
        f.close() 
        os.remove(my_file_in)
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