SELECT ins.instancecode,ins.itype,ins.src, trk.rowversion, ins.target
FROM(
	SELECT fic.instancecode,fcm.* 
	FROM framework.instanceconfig fic
	JOIN framework.collectionmaps fcm ON fic.instancetype=fcm.itype
		WHERE fcm.isactive=true AND fic.isactive=true
)ins
JOIN
(
	SELECT 
		RANK() OVER(PARTITION BY fct.collection,fct.instancecode ORDER BY fct.rowversion desc) AS rnk
		,fct.*
	FROM framework.changetracker fct
		WHERE fct.status=true
)trk
ON trk.instancecode=ins.instancecode
WHERE trk.rnk=1 --and trk.instancetype=ins.itype and ins.src=trk.collection
/*
SELECT * FROM framework.changetracker;
SELECT * FROM framework.instanceconfig;
SELECT * FROM framework.collectionmaps;
*/


CREATE OR REPLACE VIEW insinfo AS
SELECT fic.instancecode,fic.instancetype,fcm.src,fcm.target ,0::bigint as rover
FROM framework.instanceconfig fic
JOIN framework.collectionmaps fcm ON fic.instancetype=fcm.itype
	WHERE fcm.isactive=true AND fic.isactive=true

CREATE OR REPLACE VIEW cht AS
SELECT fct.instancecode,fct.instancetype,fct.collection,fct.rowversion,
	RANK() OVER(PARTITION BY fct.collection,fct.instancetype,fct.instancecode ORDER BY fct.rowversion DESC)::BIGINT as nrank
	FROM framework.changetracker fct
		WHERE fct.status=true

create table framework.insnow(
	instancecode text,
	instancetype text,
	src text,
	target text,
	rover bigint,
	primary key(instancecode,instancetype,src)
)

update framework.insnow as fin
set rover=cht.rowversion
from cht
where fin.instancecode=cht.instancecode and fin.instancetype=cht.instancetype and fin.src=cht.collection and cht.nrank=1
