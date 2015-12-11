<?php
/**
 * Piwik - Open source web analytics
 *
 * @link http://piwik.org
 * @license http://www.gnu.org/licenses/gpl-3.0.html GPL v3 or later
 *
 * @category Piwik
 * @package Piwik
 */
namespace Piwik\Db\DAO;

/**
 * @package Piwik
 * @subpackage Piwik_Db
  */
class Generic
{
    protected $db;

    public function __construct($db)
    {
        $this->db = $db;
    }

    public function getMax($table, $col)
    {
        $sql = "SELECT MAX($col) FROM $table";
        return $this->db->fetchOne($sql);
    }

    public function getCountFromWhere($table, $where)
    {
        $sql = "SELECT COUNT(*) FROM $table WHERE " . implode(' AND ', array_keys($where));
        return (int) $this->db->fetchOne($sql, array_values($where));
    }

    /**
     * Performs a non-SELECT query on a table one chunk at a time.
     * 
     * @param string $sql The SQL to perform. The last two conditions of the WHERE
     *                    expression must be as follows: 'id >= ? AND id < ?' where
     *                    'id' is the int id of the table.
     * @param int $first The minimum ID to loop from.
     * @param int $last The maximum ID to loop to.
     * @param int $step The maximum number of rows to scan in each smaller query.
     * @param array $parameters Parameters to bind in the query, array( param1 => value1, param2 => value2)
     * @return array
     */
    public function segmentedQuery($sql, $first, $last, $step, $params=array())
    {
        if ($step > 0) {
            for ($i = $first; $i <= $last; $i += $step) {
                $currentParams = array_merge($params, array($i, $i + $step));
                $this->db->query($sql, $currentParams);
            }
        }
        else {
            for ($i = $first; $i >= $last; $i += $step) {
                $currentParams = array_merge($params, array($i, $i + $step));
                $this->db->query($sql, $currentParams);
            }
        }
    }

    /**
     * Performs a SELECT on a table one chunk at a time and returns the first
     * fetched value.
     * 
     * @param string $sql The SQL to perform. The last two conditions of the WHERE
     *                    expression must be as follows: 'id >= ? AND id < ?' where
     *                    'id' is the int id of the table. If $step < 0, the condition
     *                    should be 'id <= ? AND id > ?'.
     * @param int $first The minimum ID to loop from.
     * @param int $last The maximum ID to loop to.
     * @param int $step The maximum number of rows to scan in each smaller SELECT.
     * @param array $parameters Parameters to bind in the query, array( param1 => value1, param2 => value2)
     * @return array
     */
    public function segmentedFetchFirst( $sql, $first, $last, $step, $params=array())
    {
        $result = false;
        if ($step > 0) {
            for ($i = $first; $result === false && $i <= $last; $i += $step) {
                $result = $this->db->fetchOne($sql, array_merge($params, array($i, $i + $step)));
            }
        }
        else {
            for ($i = $first; $result === false && $i >= $last; $i += $step) {
                $result = $this->db->fetchOne($sql, array_merge($params, array($i, $i + $step)));
            }
        }
        return $result;
    }

    /**
     * Performs a SELECT on a table one chunk at a time and returns an array
     * of every fetched value.
     *
     * @param string $sql    The SQL to perform. The last two conditions of the WHERE
     *                       expression must be as follows: 'id >= ? AND id < ?' where
     *                      'id' is the int id of the table.
     * @param int    $first  The minimum ID to loop from.
     * @param int    $last   The maximum ID to loop to.
     * @param int    $step   The maximum number of rows to scan in each smaller SELECT.
     * @param array  $params Parameters to bind in the query, array( param1 => value1, param2 => value2)
     *
     * @return array
     */
    public function segmentedFetchOne($sql, $first, $last, $step, $params=array())
    {
        $result = array();
        if ($step > 0) {
            for ($i = $first; $i <= $last; $i += $step) {
                $result[] = $this->db->fetchOne($sql, array_merge($params, array($i, $i + $step)));
            }
        } else {
            for ($i = $first; $i >= $last; $i += $step) {
                $result[] = $this->db->fetchOne($sql, array_merge($params, array($i, $i + $step)));
            }
        }
        return $result;
    }

    /**
     * Performs a SELECT on a table one chunk at a time and returns an array
     * of every fetched row.
     *
     * @param string $sql    The SQL to perform. The last two conditions of the WHERE
     *                       expression must be as follows: 'id >= ? AND id < ?' where
     *                      'id' is the int id of the table.
     * @param int    $first  The minimum ID to loop from.
     * @param int    $last   The maximum ID to loop to.
     * @param int    $step   The maximum number of rows to scan in each smaller SELECT.
     * @param array  $params Parameters to bind in the query, array( param1 => value1, param2 => value2)
     *
     * @return array
     */
    public function segmentedFetchAll($sql, $first, $last, $step, $params=array())
    {
        $result = array();
        if ($step > 0) {
            for ($i = $first; $i <= $last; $i += $step) {
                $currentParams = array_merge($params, array($i, $i + $step));
                $result = array_merge($result, $this->db->fetchAll($sql, $currentParams));
            }
        } else {
            for ($i = $first; $i >= $last; $i += $step) {
                $currentParams = array_merge($params, array($i, $i + $step));
                $result = array_merge($result, $this->db->fetchAll($sql, $currentParams));
            }
        }
        return $result;
    }

    /**
     * getQuoteIdentifierSymbol
     *
     * Returns the symbol/character used to quote the keywords in the sql queries.
     * Most DBMSes use ", mysql uses `
     *
     * @return string
     */
    public function getQuoteIdentifierSymbol()
    {
        return '"';
    }
}
