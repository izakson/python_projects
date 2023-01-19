select s.date_ AS date,
	OP.OP_Name as cid, 
	a.Помещение as oid, 
	s.OperTypeName as operation,
	s.Qty as qty, 
	s.tisa_Space AS sqr, 
	s.tisa_Sum as rub, 
	s.tisa_CompensationSumm AS compensation,
	s.[1kvm_space] as cost_denom,
	s.[1kvm_summ] as cost_num
from [dbo].[fnGet_Sales]('"+str(from_year)+"0101', '20221206',2, 0) S
	 join dim_Article a
		ON S.tisa_ArticleId = a.ПомещениеID
	JOIN V_Opportunity op
		 on s.OpportunityId = OP.OpportunityId
