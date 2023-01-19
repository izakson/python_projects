select [Дата] as date, 
	[Номер договора] as cid,
	[Помещение] as oid,
	[Тип операции] as operation,
	[Объем продаж, шт] as qty,
	[Объем продаж, м2] as sqr,
	[Выручка, руб] as rub,
	[Компенсация, руб] as compensation,
	[Площадь для расчета цены за м2] as cost_denom,
	[Стоимость для расчета цены за м2] as cost_num
from [tableau].[Продажи_Показатели] a
	left join [tableau].[Объект недвижимости] c
		on a.tisa_ArticleId=c.tisa_ArticleId
	left join [tableau].[Договор] d
		on a.Opportunityid=d.Opportunityid
	left join  [tableau].[Проект] e
		on a.ProjectId=e.Project_key
where ([Особый Стр.резерв] is null 
	or [Особый Стр.резерв] not in ('Спец. продажа'))
        and [Группа договоров] in ('ДДУ','ПДДУ','ДКП','ПДКП','ДУПТ')
        and [Дата]>='20220101' and [Дата]<='20221206'