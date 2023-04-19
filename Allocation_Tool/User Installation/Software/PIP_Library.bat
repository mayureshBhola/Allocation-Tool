@echo off
set LOGFILE=Library_Execution.log
call :LOG > %LOGFILE%
exit /B

:LOG
echo pip install datetime 
pip install datetime
echo pip install shutil
pip install shutil
echo pip install itertools
pip install itertools
echo pip install pandas
pip install pandas
echo pip install logging
pip install logging
echo pip install numpy
pip install numpy
echo pip install operator
pip install operator
echo pip install calendar
pip install calendar
echo pip install time
pip install time
echo pip install platform
pip install platform
echo pip install socket
pip install socket
echo pip install re
pip install re
echo pip install xml
pip install xml
echo pip install wsgiref
pip install wsgiref
echo pip install openpyxl
pip install openpyxl
echo setup completed
exit