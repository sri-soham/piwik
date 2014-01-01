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
namespace Piwik\Db\DAO\Mysql;

use Piwik\Common;
use Piwik\Tracker\GoalManager;

/**
 * @package Piwik
 * @subpackage Piwik_Db
 */
class Generic extends \Piwik\Db\DAO\Generic
{
    public function __construct($db)
    {
        parent::__construct($db);
    }

    public function getSqlRevenue($field)
    {
        return "ROUND(".$field.",".GoalManager::REVENUE_PRECISION.")";
    }

    public function deleteAll($table, $where, $orderBy, $maxRowsPerQuery, $parameters=array())
    {
        $orderByClause = $orderBy ? "ORDER BY $orderBy" : "";

        $sql = "DELETE FROM $table $where $orderByClause LIMIT " . (int)$maxRowsPerQuery;
        $totalRowsDeleted = 0;
        do {
            $rowsDeleted = $this->db->query($sql, $parameters)->rowCount();
            $totalRowsDeleted += $rowsDeleted;
        } while ($rowsDeleted >= $maxRowsPerQuery);

        return $totalRowsDeleted;
    }

    public function optimizeTables($tables)
    {
        if (!is_array($tables)) {
            $tables = array($tables);
        }

        return $this->db->query('OPTIMIZE TABLES ' . implode(', ', $tables));
    }

    public function getLock($lockname)
    {
        $sql = 'SELECT GET_LOCK(?, 1)';
        return $this->db->fetchOne($sql, array($lockname));
    }

    public function releaseLock($lockname)
    {
        $sql = 'SELECT RELEASE_LOCK(?)';
        return $this->db->fetchOne($sql, array($lockname));
    }

    public function lockTables($tablesToRead, $tablesToWrite)
    {
        if (!is_array($tablesToRead)) {
            $tablesToRead = array($tablesToRead);
        }
        if (!is_array($tablesToWrite)) {
            $tablesToWrite = array($tablesToWrite);
        }

        $lockExprs = array();
        foreach ($tablesToWrite as $table) {
            $lockExprs[] = $table . ' WRITE';
        }
        foreach ($tablesToRead as $table) {
            $lockExprs[] = $table . ' READ';
        }

        return $this->db->exec('LOCK TABLES ' . implode(', ', $lockExprs));
    }

    public function unlockAllTables()
    {
        return $this->db->exec('UNLOCK TABLES');
    }

    public function hasBlobDataType()
    {
        return $this->db->hasBlobDataType();
    }

    /**
     * Attempts to get a named lock. This function uses a timeout of 1s, but will
     * retry a set number of time.
     * 
     * @param string $lockName The lock name.
     * @param int $maxRetries The max number of times to retry.
     * @return bool true if the lock was obtained, false if otherwise.
     */
    public function getDbLock( $lockName, $maxRetries = 30 )
    {
        /*
         * the server (e.g., shared hosting) may have a low wait timeout
         * so instead of a single GET_LOCK() with a 30 second timeout,
         * we use a 1 second timeout and loop, to avoid losing our MySQL
         * connection
         */
        $sql = 'SELECT GET_LOCK(?, 1)';

        while ($maxRetries > 0) {
            if ($this->db->fetchOne($sql, array($lockName)) == '1') {
                return true;
            }
            $maxRetries--;
        }
        return false;
    }
    
    /**
     * Releases a named lock.
     * 
     * @param string $lockName The lock name.
     * @return bool true if the lock was released, false if otherwise.
     */
    public function releaseDbLock( $lockName )
    {
        $sql = 'SELECT RELEASE_LOCK(?)';

        return $this->db->fetchOne($sql, array($lockName)) == '1';
    }

    /**
     * Performs a batch insert into a specific table by iterating through the data
     *
     * NOTE: you should use tableInsertBatch() which will fallback to this function if LOAD DATA INFILE not available
     *
     * @param string  $tableName            PREFIXED table name! you must call Piwik\Common::prefixTable() before passing the table name
     * @param array   $fields               array of unquoted field names
     * @param array   $values               array of data to be inserted
     * @param bool    $ignoreWhenDuplicate  Ignore new rows that contain unique key values that duplicate old rows
     */
    public function insertIgnoreBatch($tableName, $fields, $values, $ignoreWhenDuplicate=true)
    {
        $fieldList = '('.join(',', $fields).')';
        $ignore = $ignoreWhenDuplicate ? 'IGNORE' : '';
        $params = Common::getSqlStringFieldsArray($values[0]);

        foreach ($values as $row) {
            $query = "INSERT $ignore INTO $tableName $fieldList VALUES ($params) \n";
            // In some instances $row is an associative array, to make it
            // compatible with positional parameters array, array_values($row)
            // is used.
            $this->db->query($query, array_values($row));
        }
    }

    // mysql doesn't cause error when numeric functions are used on varchar columns
    public function castToNumeric($colName)
    {
        return $colName;
    }

    // converts the binary to hexadecimal value if required
    public function bin2db($value)
    {
        return $value;
    }

    /**
     *  bin2db Raw Insert
     *
     *  Modifies binary values for insert queries that do not use parameterized
     *  queries. This is primarily for insertAll queries used in PHPUnit
     *  test cases.
     *
     *  @param  mixed $value
     *  @return string
     */
    public function bin2dbRawInsert($value)
    {
        if (!is_null($value)) {
            $value = "x'".bin2hex(substr($value, 1))."'";
        }

        return $value;
    }

    // undoes the conversion done by bin2db
    public function db2bin($value)
    {
        return $value;
    }

    // pertinent for postgresql only
    public function binaryColumn($col)
    {
        return $col;
    }

    // remove sql functions from field name
    // example: `HOUR(log_visit.visit_last_action_time)` gets `HOUR(log_visit` => remove `HOUR(` 
    public function removeFunctionFromField($field)
    {
        $pos1 = strpos($field, '(');
        if ($pos1 !== false) {
            $pos2 = strpos($field, ')');
            if ($pos2 !== false) {
                $sub = substr($field, $pos1+1, $pos2-$pos1-1);
            }
            else {
                $sub = substr($field, $pos1+1);
            }
            $sub = trim($sub);
        }
        else {
            $sub = trim($field);
        }

        return $sub;
    }

    public function isLockPrivilegeGranted()
    {
        $granted = false;
        try {
            $this->lockTables(Common::prefixTable('log_visit'), array());
            $this->unlockAllTables();
            $granted = true;
        }
        catch (\Exception $ex) {
            $granted = false;
        }

        return $granted;
    }

    public function getQuoteIdentifierSymbol()
    {
        return '`';
    }

    /**
     *  isNotEmptyOrIsZero
     *  Used in the Piwik_SegmentExpression::getSqlMatchFromDefinition for the
     *  MATCH_IS_NOT_NULL_NOR_EMPTY.
     */
    public function isNotEmptyOrIsZero($field)
    {
         return "$field <> '' OR $field = 0";
    }

    /**
     *  isEmpty
     *  Used in the Piwik_SegmentExpression::getSqlMatchFromDefinition for the
     *  MATCH_IS_NULL_OR_EMPTY
     */
    public function isEmpty($field) {
        return "$field = ''";
    }
}
