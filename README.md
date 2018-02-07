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
    host - Provide the IP of the mongodb instance installed<br>
    port - Port number on which the instance is hosted.<br>
    user - username for authentication.<br>
    passwd - password encrypted using python module 'cryptography'<br>
    pass_key - password key created using 'cryptography' module.<br>
    authentication - Toggle to ON/OFF the user authentication.<br>

  backup:<br>
    disabled: Toggle to disable the backup manager.<br>
    deferred: Toggle defer/start the backup manager.<br>
    debug: Toggle to enable the debug mode.<br>
    path: Location on disk or a UNC path to store the backup files. The user should have explicit write permission prior starting the service.<br>
    retention: Number of latest active rentention sets to store on disk. Other sets will be purged once they expire.<br>
    frequency: Hours of the day to execute the backup. Follow 24-Clock format.<br>
    day_of_week: Week number to execute the report.<br>

  index:<br>
    disabled: Toggle to disable the index manager.<br>
    deferred: Toggle defer/start the index manager.<br>
    debug: Toggle to enable the debug mode.<br>
    stats: Get stats to collections for future review.<br>
    day_of_week: Week number to execute the report.<br>

NOTE: If you're not familiar with 'cryptography' module, turn off the authentication in eaedb section.<br>
  eaedb:<br>
    authentication: False<br>

<b>KICK-OFF</b><br>
  Copy all the files in a folder.<br>
  <b>python dtsAdmin.py</b>
  
  
