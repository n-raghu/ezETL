import sys,yaml

ymlConfigEAE='eae.yml'
ymlEgDIM='dim.eg.yml'
ymlConfigDIM='newdim.yml'
ymlEgTalend='connection.example.properties'
ymlConfigTalend='connection.properties'

try:
    with open(ymlConfigEAE,'r') as yfl:
        eae=yaml.safe_load(yfl)
    with open(ymlEgDIM,'r') as yfl:
        egdim=yaml.safe_load(yfl)
    with open(ymlEgTalend,'r') as yfl:
        egTalend=yaml.safe_load(yfl)
except IOError as err:
    sys.exit(err)

for _key in egdim.keys():
    _val=eae.get(_key,False)
    if not _val:
        warnmsg='Key ' +_key+ ' Not Found. Inherited from example file.'
        print(warnmsg)
    else:
        egdim[_key].update(_val)



with open(ymlConfigDIM,'w') as yfl:
    yaml.dump(egdim,yfl,default_flow_style=False)
