CREATE TABLE [schemaName].[partititonInfo]
WITH
(
	DISTRIBUTION = REPLICATE,
	CLUSTERED INDEX
	(
		[SchemaName] ASC,
		[TableName] ASC,
		[partition_number] ASC
	)
)
AS
SELECT
    partition_number,
    TableName,
    SchemaName,
    row_count,
    left_boundary,
    right_boundary
FROM (
    SELECT 
        CAST(pnp.partition_number AS INT) partition_number
        ,CAST(t.name AS VARCHAR(255)) TableName
        ,sm.name AS SchemaName
        ,CAST(SUM(nps.[row_count]) AS BIGINT) AS [row_count]
        ,CAST(LAG(CAST(rv.[value] AS BIGINT),1) OVER(PARTITION BY sm.name ,t.name ORDER BY pnp.partition_number) AS BIGINT) AS left_boundary
        ,CAST(rv.[value] AS BIGINT) AS right_boundary
        ,CAST(0 AS INT) AS toBeDeleted
        ,CAST(0 AS INT) AS isTreated
    FROM
    sys.tables t
    JOIN    sys.schemas         AS sm ON  t.[schema_id]        = sm.[schema_id]
    INNER JOIN sys.indexes i
        ON  t.[object_id] = i.[object_id]
        AND i.[index_id] <= 1 /* HEAP = 0, CLUSTERED or CLUSTERED_COLUMNSTORE =1 */
    INNER JOIN sys.pdw_table_mappings tm
        ON t.[object_id] = tm.[object_id]
    INNER JOIN sys.pdw_nodes_tables nt
        ON tm.[physical_name] = nt.[name]
    INNER JOIN sys.pdw_nodes_partitions pnp 
        ON nt.[object_id]=pnp.[object_id] 
        AND nt.[pdw_node_id]=pnp.[pdw_node_id] 
        AND nt.[distribution_id] = pnp.[distribution_id]
    INNER JOIN sys.dm_pdw_nodes_db_partition_stats nps
        ON nt.[object_id] = nps.[object_id]
        AND nt.[pdw_node_id] = nps.[pdw_node_id]
        AND nt.[distribution_id] = nps.[distribution_id]
        AND pnp.[partition_id]=nps.[partition_id]

    JOIN        sys.data_spaces ds              ON      ds.[data_space_id]    = i.[data_space_id]
    LEFT JOIN   sys.partition_schemes ps        ON      ps.[data_space_id]    = ds.[data_space_id]
    LEFT JOIN   sys.partition_functions pf      ON      pf.[function_id]      = ps.[function_id]
    LEFT JOIN   sys.partition_range_values rv   ON      rv.[function_id]      = pf.[function_id]
                                                AND     rv.[boundary_id]      = nps.[partition_number]
    WHERE 1=1
    AND sm.name IN ('mySchema') AND t.name in ('table1', 'table2')
    GROUP BY 
        pnp.partition_number
        ,sm.name 
        ,t.name
        ,rv.[value]
) p