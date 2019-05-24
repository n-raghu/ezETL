# DATA INTEGRATION MANAGEMENT
### GUIDE TO SETUP ENVIRONMENT AND START WITH EXECUTION
#### Create sudo user

1. Lets assume the created user is <sudouser>

#### Update YUM & install YUM UTILS
```
sudo yum -y update
sudo yum -y install yum-utils
sudo yum -y groupinstall development
sudo yum install -y python36u-devel.x86_64 postgresql-devel postgresql-libs
```

#### Setup Python 3 & PIP
```
sudo yum -y install https://centos7.iuscommunity.org/ius-release.rpm
sudo yum -y install python36u
sudo yum -y install python36u-pip
```

#### Create environment for Data Integration
```
sudo mkdir /dim
sudo chown -R <sudouser>:<sudouser> /dim
python3.6 -m venv dim
```

#### Activate the environment
```
source dim/bin/activate
```
_You should see name of the environment in the shell i.e. (dim), this indicates that environment is successfully created and activated_

#### Install & configure MS connection
```
sudo su
curl https://packages.microsoft.com/config/rhel/7/prod.repo > /etc/yum.repos.d/mssql-release.repo
exit
sudo yum remove unixODBC-utf16 unixODBC-utf16-devel
sudo ACCEPT_EULA=Y yum install -y msodbcsql17
```
Check the location where MS Driver is installed by using _odbcinst -j_ command and copy the location which shows **Drivers....** (mostly in the first line)
```
odbcinst -j
sudo nano <config file>
```
Now find the MSSQL driver and the first line has the name of the driver enclosed in square brackets **[]** and change the name to MSSQL-17 which looks like **[MSSQL-17]**

_Once the MS ENVIRONMENT is ready we are ready for the next steps_

#### Upgrade PIP and install DIM modules
```
pip install --upgrade pip
pip install -r modules
```

#### Install RAY
**Note: DO NOT ADD THESE LIBRARIES IN MODULES FILE FOR AUTOMATED/SILENT INSTALL. REQUIRES MANUAL INSTALL**
```
pip install -U ray
pip install -U ray[debug]
```
