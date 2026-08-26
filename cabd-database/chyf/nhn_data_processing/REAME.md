# NHN Work Unit Processing Overview


This is a brief overview of the steps required for processing and loading NHN work units into CWF CHyF database.

## References

https://cwffcf.sharepoint.com/:w:/r/sites/CABD/_layouts/15/Doc.aspx?sourcedoc=%7B7F54AF66-8825-4F55-8E7E-F2823A50265D%7D&file=NHN%20to%20CHyF%20Data%20Processing%20Workflow.docx&action=default&mobileredirect=true


## Processing Steps

**Step 1: Download raw NHN data**

https://ftp.maps.canada.ca/pub/nrcan_rncan/vector/geobase_nhn_rhn/gdb_en/

**Step 2: Load Data**

nhn_data_load.py

`nhn_data_load.py <host> <port> <database> <user> <password> <nhnworkunit> <zipfile>`

**Step 3: QA Data and Manually Fix Results (Repeat as necessary)**

nhn_data_qa.py 

`nhn_data_qa.py <host> <port> <database> <user> <password> <nhnworkunit>`

**Step 4: Prepare for Flowpath Tools**

nhn_2_fpprocessing.py

`nhn_2_fpprocessing.py <host> <port> <database> <user> <password> <nhnworkunit>`

This copies the data into the `fpinput` schema where (if running) the CHyF processing server should pick up the AOI and run the flowpath tools on it.

**Step 5: Repeat 1-4 for Next Work Unit**

**Step 6: Flowpath Processing - Running **

In Step4 the AOI should be automatically setup for processing. To review the results look at the `fpoutput.aoi` table status column. Further details are in the reference document.

**Step 7: Flowpath Processing - Review Results**

Review the flowpath processing results and ensure no issues need to be dealt with. This should include: 
 * Reviewing the status field in the `fpoutput.aoi` table to ensure all aois have a value of FP_DONE 
 * Reviewing the records of the `fpoutput.errors` table to ensure there are no items that need to be dealt with 



 **Step 8 – Check for "Close Nodes"**


 **Step 9 – Copy Processed WorkUnit to CHyF Model**

This should generally be run after a number of workunits are completed and ready for processing. This will copy the data from the `fpinput` schema into the `chyf2` schema. Only AOI's with a status of `CHYF_READY` will be copied.

flowpath_2_chyf.py

`flowpath_2_chyf.py <host> <port> <dbname> <dbuser> <dbpassword> fpinput`

 **Step 10 – Run the Mainsteam Tools**

 This adds the graph_id, mainstems and other properties to the network.

Scope: This needs to be run for the entire database. 

Runtime: ~1 day

Java Version: Java 11

Current Version: 1.3.3

This tool can be run on either CHyF Processing Server.  You could try running it locally, but it will likely take too long and too many resources.

In the following commands you need to change the database host, user, and password (and perhaps append the date to the log.txt file):

    cd /home/azureuser/chyf-streamorder-1.3.3-20260804

    /usr/lib/jvm/java-11-openjdk-amd64/bin/java -Djava.io.tmpdir=/mnt  -XX:MaxMetaspaceSize=512m -XX:MaxDirectMemorySize=512m  -Xmx4G  -cp ./lib/*:./lib-chyf/chyf-core-1.5.10.jar:./lib-chyf/chyf-streamorder-1.3.3.jar net.refractions.chyf.streamorder.StreamOrderComputer -d "host=<HOST>;port=5432;db=chyf;user=<USER>;password=<PASSWORD>"  -singlenames -pagecachesize 1g chyf2 chyf2.eflowpath_properties > log.txt




 **Step 11 – Run the Raw Elevation Tools**

This computes and adds raw elevation for the network (setting the Z value in the linestrings).

Scope: This can be run for only the updated AOIs (as long as all the existing AOI's have had raw elevation applied).  To limit the scope to specific AOI's edit the `elevation.properties` file and at the bottom configure the AOI_FILTER property (comma delimited list of workunit short names)

Runtime: 1-2 days

Java Version: Java 25

This tool can be run on either CHyF Processing Server. 

In the following commands you need to change the database host, user (`chyf_processor` can be used), and password (and perhaps append the date to the log.txt file):

    cd /home/azureuser/chyf-elevation-1.0.4-20260817

On CHyF Processing Server:

    /usr/lib/jvm/jdk-25.0.3+9/bin/java -cp lib/*:lib-chyf/chyf-elevation-1.0.4.jar net.refractions.chyf.elevation.raw.ElevationEngine -d "host=<HOST>;port=5432;db=chyf;user=<USER>;password=<PASSWORD>" elevation.properties > log.txt

On CHyF Processing Server 2:

    java -cp lib/*:lib-chyf/chyf-elevation-1.0.4.jar net.refractions.chyf.elevation.raw.ElevationEngine -d "host=<HOST>;port=5432;db=chyf;user=<USER>;password=<PASSWORD>" elevation.properties > log.txt


Processing is done in blocks, controlled by the `public.elevation_processing` table. If processing dies for any reason you can restart it by modifying the values in that table and using the `-docontinue` parameter: `java -... ElevationEngine -d ... -docontinue elevation.properties ...`

The `elevation.properties` file controls various settings for the elevation processing. You can modify these if necessary.

 **Step 12 – Run the Smoothed Elevation Tools**

 This computes and adds the smoothed elevation for the network (setting the M value in the linestrings).

 Scope: This needs to be run for the entire database.

 Runtime: 2-4 days

Java Version: Java 25

This tool can be run on either CHyF Processing Server.  

In the following commands you need to change the database host, user (`chyf_processor` can be used), and password (and perhaps append the date to the log.txt file):

    cd /home/azureuser/chyf-elevation-1.0.4-20260817

On CHyF Processing Server:

    /usr/lib/jvm/jdk-25.0.3+9/bin/java -cp lib/*:lib-chyf/chyf-elevation-1.0.4.jar net.refractions.chyf.elevation.smooth.ZSmoothingEngine -d "host=<HOST>;port=5432;db=chyf;user=<USER>;password=<PASSWORD>" elevation.properties > log.txt

On CHyF Processing Server 2:

    java -cp lib/*:lib-chyf/chyf-elevation-1.0.4.jar net.refractions.chyf.elevation.smooth.ZSmoothingEngine -d "host=<HOST>;port=5432;db=chyf;user=<USER>;password=<PASSWORD>" elevation.properties > log.txt


Processing is done in blocks, controlled by the `public.elevation_smoothing` table. If processing dies for any reason you can restart it by modifying the values in that table and using the `-docontinue` parameter: `java -... ZSmoothingEngine -d ... -docontinue elevation.properties ...`

The `elevation.properties` file controls various settings for the elevation processing. You can modify these if necessary.

 **Step 13 – Reset Database Types**

Run the sql in the `flowpath_after_elevation.sql` to ensure the geometry column types are configured correctly.
