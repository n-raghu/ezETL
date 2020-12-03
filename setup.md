# DATA INTEGRATION MANAGEMENT
### GUIDE TO SETUP ENVIRONMENT AND START WITH EXECUTION
#### Create sudo user

1. Lets assume the created user is _sudouser_

#### Update YUM & install YUM UTILS
```
sudo yum -y update
sudo yum -y install yum-utils
sudo yum -y groupinstall development
sudo yum install -y postgresql-devel postgresql-libs
```

#### Setup Python 3 & PIP
```
sudo yum -y install https://centos7.iuscommunity.org/ius-release.rpm
INSTALL python3.6.9 and PIP
```

#### Create environment for Data Integration
```
sudo mkdir /home/centos/eaedim2
sudo chown -R <sudouser>:<sudouser> /eaedim2
python3.6 -m venv eaedim2
```

#### Activate the environment
```
source eaedim2/bin/activate
```
_You should see name of the environment in the shell i.e. (eaedim2), this indicates that environment is successfully created and activated_

#### Upgrade PIP and install DIM modules
```
pip install --upgrade pip
pip install -r modules
```
