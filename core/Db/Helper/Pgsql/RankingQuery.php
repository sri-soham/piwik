<?php
/**
 * Piwik - Open source web analytics
 *
 * @link http://piwik.org
 * @license http://www.gnu.org/licenses/gpl-3.0.html GPL v3 or later
 * @version $Id: RankingQuery.php 6980 2012-09-13 02:22:04Z capedfuzz $
 *
 * @category Piwik
 * @package Piwik
 */

/**
 * The ranking query class wraps an arbitrary SQL query with more SQL that limits 
 * the number of results while grouping the rest to "Others" and allows for some 
 * more fancy things that can be configured via method calls of this class. The
 * advanced use cases are explained in the doc comments of the methods.
 * 
 * The general use case looks like this:
 * 
 * // limit to 500 rows + "Others"
 * $rankingQuery = new Piwik_RankingQuery(500);
 * 
 * // idaction_url will be "Others" in the row that contains the aggregated rest
 * $rankingQuery->addLabelColumn('idaction_url');
 * 
 * // the actual query. it's important to sort it before the limit is applied
 * $sql = 'SELECT idaction_url, COUNT(*) AS nb_hits
 *         FROM log_link_visit_action
 *         GROUP BY idaction_url
 *         ORDER BY nb_hits DESC';
 * 
 * // execute the query
 * $rankingQuery->execute($sql);
 * 
 * 
 * For more examples, see RankingQueryTest.php
 * 
 * 
 * @package Piwik
 */
class Piwik_Db_Helper_Pgsql_RankingQuery extends Piwik_Db_Helper_Mysql_RankingQuery
{
	/**
	 * Generate the SQL code that does the magic.
	 * If you want to get the result, use execute() instead. If you're interested in
	 * the generated SQL code (e.g. for debugging), use this method.
	 * 
	 * @param $innerQuery string  SQL of the actual query
	 * @return string             entire ranking query SQL 
	 */
	public function generateQuery($innerQuery)
	{
		// generate select clauses for label columns
		$tmp = array();
		// tmp after limit
		// this is required to get the aggregates for the rows that over and
		// above the limit clause for all types
		$tmp_al = array(); // tmp after limit
		foreach ($this->labelColumns as $column => $val) {
			$tmp[] = $this->db->quoteIdentifier($column);
			$tmp_al[] = "'".$this->othersLabelValue."' AS ".$this->db->quoteIdentifier($column);
		}
		$labelColumnsString = implode(', ', $tmp);
		$afterLimitLabelColumnsString = implode(', ', $tmp_al);
		
		// generate select clauses for additional columns
		$additionalColumnsString = '';
		$additionalColumnsAggregatedString = '';
		$groupByColumnsString = $labelColumnsString;
		foreach ($this->additionalColumns as $column => $aggregation) {
			$additionalColumnsString .= ', ' . $this->db->quoteIdentifier($column);
			if ($aggregation !== false)
			{
				$additionalColumnsAggregatedString 
					.= ', '.$aggregation.'('.$this->db->quoteIdentifier($column).') '
					  . 'AS '.$this->db->quoteIdentifier($column);
			}
			else
			{
				$additionalColumnsAggregatedString .= ', '.$this->db->quoteIdentifier($column);
				$groupByColumnsString .= ', '.$this->db->quoteIdentifier($column);
			}
		}
		
		$with_query = "WITH actual_query AS ( \n" . $innerQuery . "\n), \n";
		// initialize the counters
		if ($this->partitionColumn !== false)
		{
			/**
			 *	These additional common table expressions in $parts1 are required to retain
			 *	the ordered result set. 
			 *	Using the actual_query with where and union all like
			 *		with actual_query as ($innerQuery) 
			 *		select $labelColumnsString.$additionalColumnsString
			 *		from actual_query where partioncolumn=value limit 50000
			 *		union all
			 *		select $labelColumnsString.$additionalColumnsString
			 *		from actual_query where partioncolumn=value limit 50000
			 *	is disrupting the ordered result set and returning a result
			 *	set that is different from what is expected.
			 */
			$parts11 = array(); // this is for aggregates of rows within the $this->limit
			$parts12 = array(); // this is for aggregates of rows after the $this->limit
			
			// subquery alias for the query with "where $this->partitionColumn is null"
			$query_null = 'query_null';
			foreach ($this->partitionColumnValues as $value)
			{
				$tmp = '( SELECT '.$labelColumnsString.$additionalColumnsString.' FROM actual_query '
					 . ' WHERE ' . $this->partitionColumn.' = '.intval($value);
				$parts11[] = ' query'.$value.' AS '
						 . $tmp
						 . ' LIMIT '.$this->limit.')';
				$parts12[] = ' query'.$value.$value.' AS '
					       . $tmp
						   . ' OFFSET ' . $this->limit.')';
			}
			$parts11[] = ' '.$query_null.' AS ( SELECT '.$labelColumnsString.$additionalColumnsString
						.' FROM actual_query WHERE '.$this->partitionColumn.' IS NULL ) ';

			$parts21 = array(); // this is for aggregates of rows within $this->limit
			$parts22 = array(); // this is for aggregates of rows after $this->limit
			foreach ($this->partitionColumnValues as $value)
			{
				$parts21[] = 'SELECT * FROM query'.$value;
				$parts22[] = 'SELECT ' . $afterLimitLabelColumnsString.$additionalColumnsString
							. ' FROM query'.$value.$value;
			}
			$parts21[] = 'SELECT * FROM '.$query_null;
			// aggregates of rows outside the limit are part of the "tmp_query" sub query
			$sql = $with_query
				  . implode(",\n", $parts11) . ",\n"
				  . implode(",\n", $parts12) . "\n"
				  . implode(" UNION ALL \n", $parts21) . " UNION ALL\n"
				  . 'SELECT ' . $afterLimitLabelColumnsString.$additionalColumnsAggregatedString . " FROM (\n"
				  . "\t".implode(" UNION ALL \n\t", $parts22)
				  . "\n) AS tmp_query GROUP BY " . $groupByColumnsString;
		}
		else
		{
			$sql = $with_query
				 . 'SELECT '.$labelColumnsString.$additionalColumnsString
				 . ' FROM actual_query LIMIT ' . $this->limit;
		}
		
		return $sql;
	}
}
