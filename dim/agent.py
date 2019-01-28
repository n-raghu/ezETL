import subprocess as sbp
from datetime import datetime as dtm

proc=sbp.Popen(['python','-V',]stdout=sbp.PIPE,stderr=sbp.PIPE)
proc.wait()
stdio,stder=proc.communicate()

print(stdio)
print(stder)

# v1
# EXEC CHILD-JOB popStagingTables
# Create JSON config file
# EXEC CHILD-JOB cacheIndex
# EXEC TALEND-JOB -1
# EXEC TALEND-JOB -2
