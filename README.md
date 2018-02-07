<b>automatedBKP</b><br>
This repository is created for automating and managing indices and backups for mongoDB.

Project is integrated with YAML based inputs while controls the entire functioning.

<b>CONFIGURE</b><br>
  Open dtsConfig.yaml file
  You should see three section in the config file
  eaedb - Configuration of the database and the settings are stored here.
  backup - Configuration of the backup and the settings are stored here.
  index - Configuration of the index and the settings are stored here.
  
  eaedb:<br>
    host - Provide the IP of the mongodb instance installed
    port - Port number on which the instance is hosted.
    user - username for authentication
    passwd - password encrypted using python module 'cryptography'
    pass_key - password key created using 'cryptography' module.
    authentication - Toggle to ON/OFF the user authentication.

  backup:<br>
    disabled: Toggle to disable the backup manager.
    deferred: Toggle defer/start the backup manager.
    debug: Toggle to enable the debug mode.
    path: Location on disk or a UNC path to store the backup files. The user should have explicit write permission prior starting the service.
    retention: Number of latest active rentention sets to store on disk. Other sets will be purged once they expire.
    frequency: Hours of the day to execute the backup. Follow 24-Clock format.
    day_of_week: Week number to execute the report.

  index:<br>
    disabled: Toggle to disable the index manager.
    deferred: Toggle defer/start the index manager.
    debug: Toggle to enable the debug mode.
    stats: Get stats to collections for future review.
    day_of_week: Week number to execute the report.

NOTE: If you're not familiar with 'cryptography' module, turn off the authentication in eaedb section.
  eaedb:
    authentication: False

<b>KICK-OFF</b><br>
  Copy all the files in a folder.<br>
  <b>python dtsAdmin.py</b>
  
  
