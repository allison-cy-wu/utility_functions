Author: Rich Winkler

The scripts within this folder will assist in identifying any locations where environments have been hardcoded in scripts and should be changed when migrating to production.

To run:
	1. First, if you don't already have a Databricks CLI profile named test_profile pointed at the tfs.cloud.databricks.com instance, please run Create_Test_Profile.bat. This script will walk you through the process of creating the test_profile.	
	2. Next, run Refresh_From_Databricks_TST.bat. This will refresh all of the code from Databricks non prod instance, and then run the two powershell scripts.
		a. Check_For_Environment.ps1 runs a grep on environment=’TST’ (or DEV or PRD) saving the output into a .txt file named environment_chk.txt
		b. Check_For_DB_Environment.ps1 runs a grep on db_environment=’test’ (or prod) saving the output into a .txt file named databricks_environment_chk.txt
	3. Last, examine environment_chk.txt and databricks_environment_chk.txt for all instances to ensure that all environment hardcodings have been switched to production.
