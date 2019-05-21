import sys,yaml

with open('eae.yml','r') as yfl:
    eae=yaml.safe_load(yfl)

with open('dim.eg.yml','r') as yfl:
    egdim=yaml.safe_load(yfl)

for _key in egdim.keys():
    _val=eae.get(_key,False)
    if not _val:
        warnmsg='Key ' +_key+ ' Not Found. Inherited from example file.'
        print(warnmsg)
    else:
        egdim[_key].update(_val)

with open('newdim.yml','w') as yfl:
    yaml.dump(egdim,yfl,default_flow_style=False)
