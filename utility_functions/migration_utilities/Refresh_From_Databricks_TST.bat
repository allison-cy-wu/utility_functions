call C:/Users/u00xbg/AppData/Local/Continuum/anaconda3/Scripts/activate.bat
databricks workspace export_dir /DS_CCG/RSD_COE/Data_Scientists . -o --profile test_profile

Powershell.exe -executionpolicy remotesigned -File  .\Check_For_Environment.ps1
Powershell.exe -executionpolicy remotesigned -File  .\Check_For_DB_Environment.ps1

ECHO "EXPORT COMPLETE"
PAUSE
exit