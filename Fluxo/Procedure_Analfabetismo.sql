USE BD_SERASA
GO
--***********INICIO  - ETAPA CRIANDO TABELA DE PROJEÇÃO 2023 POPULACIONAL E #Tb_Populacao_Unificada

CREATE PROCEDURE ATU_ESTUDO_ANALFABETISMO AS 

--Tabela temporária com a unificação das fontes IBGE e IPEADATA
IF OBJECT_ID('tempdb..#Tb_Unif_Pop_01') is not null
	DROP TABLE #Tb_Unif_Pop_01

	SELECT
		p3.* INTO #Tb_Unif_Pop_01 FROM (
	SELECT
		p1.[sigla],
		p1.ano,
		p1.valor
	FROM
		[dbo].[Tb_Populacao] as p1
	UNION
	SELECT
		p2.[sigla],
		p2.ano,
		p2.valor
	FROM
		[dbo].[Tb_Populacao_Ibge] as p2
	) as p3 

--Variação Percentual entre os meses
/*Os anos anteriores a 2022 são informações de estimativas do IBGE
desta forma usando 2019 a 2021 para encontrar a variação percentual e aplicar 
 a média no ano de 2022(fonte IPEADATA). Assim estimando a base populacional do ano 
 2023
 */
IF OBJECT_ID('tempdb..#Tb_Unif_Pop_2023') is not null
	DROP TABLE #Tb_Unif_Pop_2023

	WITH CTE_POP_01 AS 
	(
		SELECT  
		p1.sigla,
		p1.ano,
		p1.valor,
		lag(p1.valor) over( partition by p1.sigla order by p1.sigla, p1.ano asc) as valor_anterior,
		(p1.valor - lag(p1.valor) over(partition by p1.sigla order by p1.sigla, p1.ano asc)) / (lag(p1.valor) over(partition by p1.sigla order by p1.sigla, p1.ano asc)) as variacao_percentual 
		FROM #Tb_Unif_Pop_01 as p1
		WHERE p1.ano>=2019 and p1.ano <= 2021
	),
	CTE_POP_MEDIA_PERCENTUAL AS
	(
		SELECT 
		p2.sigla,
		avg(p2.variacao_percentual) as media_variacao_percentual,
		(
		SELECT 
			p3.valor 
			FROM #Tb_Unif_Pop_01 AS P3 
			WHERE p3.sigla = p2.sigla and p3.ano = (Select max(p4.ano) FROM #Tb_Unif_Pop_01 as p4)
		
		) AS ultimo_valor_pop
		FROM CTE_POP_01 as p2 
		WHERE p2.valor_anterior is not null
		GROUP BY p2.sigla
	)
	--estimativa populaciona 2023
	SELECT 
	p5.sigla,
	p5.ultimo_valor_pop + (p5.media_variacao_percentual * p5.ultimo_valor_pop) as valor, --novo valor usando a média dos últimos 2 meses
	p5.media_variacao_percentual, -- média da variação percentual
	p5.ultimo_valor_pop, -- último ano com a volumetria da população
	p6.estado,
	'2023' as ano
	INTO #Tb_Unif_Pop_2023
	FROM CTE_POP_MEDIA_PERCENTUAL AS p5
	left join Tb_de_para_Sigla as p6 on p6.sigla = p5.sigla
;

--Criando a tabela final com o volume populacional que será usada para confrontar com as demais informações
IF OBJECT_ID('tempdb..#Tb_Populacao_Unificada') is not null
	DROP TABLE #Tb_Populacao_Unificada

SELECT P7.* INTO #Tb_Populacao_Unificada FROM
(
--IPEADATA
SELECT 
	IP_DATA.ano,
	IP_DATA.sigla,
	IP_DATA.estado,
	IP_DATA.valor
	FROM [dbo].[Tb_Populacao] AS IP_DATA
UNION
--IBGE
SELECT 
	IP_DATA.ano,
	IP_DATA.sigla,
	P6.estado,
	IP_DATA.valor
	FROM [dbo].[Tb_Populacao_Ibge] AS IP_DATA
	left join Tb_de_para_Sigla as p6 on p6.sigla = IP_DATA.sigla
--ESTIMATIVA 2023
UNION
SELECT 
	IP_DATA.ano,
	IP_DATA.sigla,
	IP_DATA.estado,
	IP_DATA.valor
	FROM [dbo].[#Tb_Unif_Pop_2023] AS IP_DATA
) AS P7
WHERE P7.ano>=2019

--***********FIM  - ETAPA CRIANDO TABELA DE PROJEÇÃO 2023 POPULACIONAL E #Tb_Populacao_Unificada

--------***********INICIO ELABORAÇÃO DOS INDICADORES***********--------------------------


--Apaga tabela temporária, espelho da base final Tb_Indicador
IF OBJECT_ID ('tempdb..#Tb_Indicador_Old') is not null
	DROP TABLE #Tb_Indicador_Old

--Cria Tabela Temporária	
CREATE TABLE #Tb_indicador_Old 
(
ano int not null,
sigla char(2) not null,
regiao char(20),
estado char(20),
valor float,
valor_anl_negros_prados float,
valor_anl_brancos float,
valor_anl_homem float,
valor_anl_mulher float,
valor_pobreza float,
valor_indice_gini float,
valor_absoluto_disparidade float,
valor_despesa_educacao float,
valor_populacional float,
valor_pib float,
tipo_indicador char(50) not null,
primary key (sigla, ano, tipo_indicador)
)


-- VISÃO - ANALFABETISMO NACIONAL --
INSERT INTO #Tb_indicador_Old
(ano,
sigla,
valor,
tipo_indicador
)
SELECT 
P1.ano,
P1.sigla,
P1.valor,
'ANALFABETISMO - VISÃO BRASIL' AS tipo_indicador
FROM [dbo].[Tb_Analfabetismo_Nacional] AS P1

--VISAO ANALFABETISMO GENERO HOMENS
INSERT INTO #Tb_indicador_Old
(ano,
sigla,
valor,
tipo_indicador
)
SELECT 
P1.ano,
P1.sigla,
P4.valor as valor_anl_homem,
'ANALFABETISMO - VISÃO BRASIL GENERO HOMENS' AS tipo_indicador
FROM [dbo].[Tb_Analfabetismo_Nacional] AS P1
INNER JOIN [dbo].[Tb_Analfabetismo_Homem_Nacional] AS P4 ON P4.ano = P1.ano

--VISAO ANALFABETISMO GENERO MULHERES
INSERT INTO #Tb_indicador_Old
(ano,
sigla,
valor,
tipo_indicador
)
SELECT 
P1.ano,
P1.sigla,
P4.valor as valor_anl_mulheres,
'ANALFABETISMO - VISÃO BRASIL GENERO MULHERES' AS tipo_indicador
FROM [dbo].[Tb_Analfabetismo_Nacional] AS P1
INNER JOIN [dbo].[Tb_Analfabetismo_Mulher_Nacional] AS P4 ON P4.ano = P1.ano

--VISAO ANALFABETISMO RAÇA PRETOS/PARDOS
INSERT INTO #Tb_indicador_Old
(ano,
sigla,
valor,
tipo_indicador
)
SELECT 
P1.ano,
P1.sigla,
P4.valor as valor_anl_homem,
'ANALFABETISMO - VISÃO BRASIL RAÇA PRETOS/PARDOS' AS tipo_indicador
FROM [dbo].[Tb_Analfabetismo_Nacional] AS P1
INNER JOIN [dbo].[Tb_Analfabetismo_Negros_Pardos] AS P4 ON P4.ano = P1.ano

--VISAO ANALFABETISMO RAÇA BRANCOS
INSERT INTO #Tb_indicador_Old
(ano,
sigla,
valor,
tipo_indicador
)
SELECT 
P1.ano,
P1.sigla,
P4.valor as valor_anl_mulheres,
'ANALFABETISMO - VISÃO BRASIL BRANCOS' AS tipo_indicador
FROM [dbo].[Tb_Analfabetismo_Nacional] AS P1
INNER JOIN [dbo].[Tb_Analfabetismo_Brancos] AS P4 ON P4.ano = P1.ano

-- VISÃO - ANALFABETISMO REGIAO --
INSERT INTO #Tb_indicador_Old
(
ano,
sigla,
regiao,
valor,
tipo_indicador
)
SELECT 
P1.ano,
P1.sigla,
P1.regiao,
P1.valor,
'ANALFABETISMO - VISÃO REGIAO' AS tipo_indicador
FROM [dbo].[Tb_Analfabetismo_Regiao] AS P1

-- VISÃO - ANALFABETISMO UF --
INSERT INTO #Tb_indicador_Old
(
ano,
sigla,
estado,
regiao,
valor,
valor_pobreza,
valor_indice_gini,
valor_absoluto_disparidade,
valor_despesa_educacao,
valor_populacional,
valor_pib,
tipo_indicador
)
SELECT 
P1.ano,
P1.sigla,
P1.estado,
P2.regiao,
P1.valor,
P3.valor as valor_pobreza,
P4.valor as valor_indice_gini,
P4.valor * P6.valor AS valor_absoluto_disparidade,
P5.valor as valor_despesa_educacao,
p6.valor as valor_populacional,
P7.valor as valor_pib,
'ANALFABETISMO - VISÃO UF' AS tipo_indicador
FROM [dbo].[Tb_Analfabetismo_UF] AS P1
LEFT JOIN [dbo].[Tb_de_para_Sigla] AS P2 ON P2.SIGLA = P1.sigla
LEFT JOIN [dbo].[Tb_Pobreza_UF] AS P3 ON P3.sigla = P1.sigla AND P3.ano = P1.ano
LEFT JOIN [dbo].[Tb_Gini_UF] AS P4 ON P4.sigla = P1.sigla AND P4.ano = P1.ano
LEFT JOIN [dbo].[Tb_Despesa_Educ_UF] AS P5 ON P5.sigla = p1.sigla and P5.ano = P1.ano
LEFT JOIN #Tb_Populacao_Unificada AS P6 ON P6.sigla = P4.sigla AND P6.ano = P4.ano
LEFT JOIN [dbo].[Tb_Pib_Per_Capita] AS P7 ON P7.sigla = P1.sigla AND cast(P7.ano as int) = P1.ano


--DELETA BASE COM OS INDICADORES 
TRUNCATE TABLE Tb_indicador

--INSERE OS REGISTROS
INSERT INTO Tb_indicador
( 
ano,
sigla,
regiao, 
estado, 
valor, 
valor_anl_negros_prados,
valor_anl_brancos,
valor_anl_homem,
valor_anl_mulher,
valor_pobreza,
valor_indice_gini,
valor_absoluto_disparidade,
valor_despesa_educacao,
valor_populacional,
valor_pib,
tipo_indicador,
data_carga,
data_referencia

)
SELECT 
ano,
sigla,
regiao, 
estado, 
valor, 
valor_anl_negros_prados,
valor_anl_brancos,
valor_anl_homem,
valor_anl_mulher,
valor_pobreza,
valor_indice_gini,
valor_absoluto_disparidade,
valor_despesa_educacao,
valor_populacional,
valor_pib,
tipo_indicador,
GETDATE() as data_carga,
cast(CONCAT(ano, '-12', '-01') as date) as dt
FROM #Tb_indicador_Old


--Script tabela Indicador
/*CREATE TABLE Tb_indicador 
(
ano int not null,
sigla char(2) not null,
regiao char(20),
estado char(20),
valor float,
valor_anl_negros_prados float,
valor_anl_brancos float,
valor_anl_homem float,
valor_anl_mulher float,
valor_pobreza float,
valor_indice_gini float,
valor_absoluto_disparidade float,
valor_despesa_educacao float,
valor_populacional,
tipo_indicador char(50) not null,
data_carga datetime,
data_referencia date,
primary key (sigla, ano, tipo_indicador)
)*/






