<?
/**
 *  Piwik _ Open source web analytics
 *
 *  @link http://piwik.org
 *  @license http://www.gnu.org/licenses/gpl-3.0.html GPL v3 or later
 *
 *  @category Piwik
 *  @package Piwik
 */
namespace Piwik\Db\DAO\Pgsql;

use Piwik\Common;
use Piwik\Db\Schema;
use Piwik\Db\Factory;

/**
 *  @package Piwik
 *  @subpackage Piwik_Db
 */

class Archive extends \Piwik\Db\DAO\Mysql\Archive
{
    private $Generic;
    private $isBlobTable;

    public function __construct($db, $table)
    {
        parent::__construct($db, $table);
    }

    /**
     *  In postgresql indexes cannot be part of the create table statement. 
     *  They have to be created separately
     */
    public function createPartitionTable($tableName, $generatedTableName)
    {
        $sql = $this->getPartitionTableSql($tableName, $generatedTableName);
        $this->db->query($sql);

        $indexes = Schema::getInstance()->getIndexesCreateSql();
        $indexes = $indexes[$tableName];

        foreach ($indexes as $index) {
            $index = str_replace('#table#', $generatedTableName, $index);
            $this->db->query($index);
        }
    }

    public function loadNextIdarchive($table, $alias, $locked, $idsite, $date)
    {
        $dbLockName = $this->lockNameForNextIdarchive($table);
        $Generic = Factory::getGeneric($this->db);

        if ($Generic->getDbLock($dbLockName) === false) {
            throw new Exception('loadNextIdarchive: Cannot get lock on table '. $table);
        }

        $sql = "INSERT INTO $table "
             . '   SELECT COALESCE(MAX(idarchive),0)+1 '
             . "        , '$locked' "
             . "        , $idsite "
             . "        , '$date' "
             . "        , '$date' "
             . '        , 0 '
             . "        , '$date' "
             . '        , 0 '
             . "   FROM $table AS $alias";
        $this->db->exec($sql);
        
        $Generic->releaseDbLock($dbLockName);
    }

    public function getByIdarchiveName($table, $idarchive, $name)
    {
        $valueCol = $this->prepareForBinary($table);
        $sql = "SELECT $valueCol, ts_archived FROM $table "
             . 'WHERE idarchive = ? AND name = ?';
        $row = $this->db->fetchRow($sql, array($idarchive, $name));
        
        return $this->binaryOutput($row);
    }

    public function getAllByIdarchiveNameLike($table, $idarchive, $name)
    {
        $nameEnd = strlen($name) + 2;
        $valueCol = $this->prepareForBinary($table);
        $sql = "SELECT $valueCol, name FROM $table 
                WHERE idarchive = ? AND
                        (name = ? OR
                            (name LIKE ? AND
                            SUBSTRING(name FROM $nameEnd FOR 1) >= '0' AND
                            SUBSTRING(name FROM $nameEnd FOR 1) <= '9'
                            )
                        )";
        $rows = $this->db->fetchAll($sql, array($idarchive, $name, $name.'%'));

        return $this->binaryOutput($rows, true);
    }

    public function getByIdsNames($table, $archiveIds, $fields)
    {
        $valueCol = $this->prepareForBinary($table);
        $inNames = Common::getSqlStringFieldsArray($fields);
        $sql = "SELECT $valueCol, name, idarchive, idsite FROM $table "
             . "WHERE idarchive IN ($archiveIds) "
             . "  AND name IN ($inNames)";
        $rows = $this->db->fetchAll($sql, $fields);

        return $this->binaryOutput($rows, true);
    }

    public function insertRecord($tableName, $bind)
    {
        $Generic = Factory::getGeneric($this->db);

        $params = Common::getSqlStringFieldsArray($bind);
        $sql = 'INSERT INTO ' . $tableName . '( ' . implode(', ', array_keys($bind)) . ' ) '
             . ' VALUES ( ' . $params . ' ) ';

        if ($this->isBlob($tableName) && isset($bind['value'])) {
            $bind['value'] = $Generic->bin2db($bind['value']);
        }
        $Generic->insertIgnore($sql, array_values($bind));
    }

    public function insertIgnoreBatch($tableName, $fields, $values, $ignoreWhenDuplicate)
    {
        if (array_values($values[0]) === $values[0]) {
            $valueIndex = array_search('value', $fields);
        }
        else {
            $valueIndex = array_search('value', $fields);
            if ($valueIndex !== false) {
                $valueIndex = 'value';
            }

        }
        if ($this->isBlob($tableName) && $valueIndex !== false) {
            $Generic = Factory::getGeneric($this->db);
            while (list($k, $row) = each($values)) {
                $values[$k][$valueIndex] = $Generic->bin2db($row[$valueIndex]);
            }
        }
        

        if ($ignoreWhenDuplicate) {
            $tmp_table = 'tmp_' . $tableName;
            $sql = 'CREATE TEMPORARY TABLE ' . $tmp_table . ' '
                 . ' AS SELECT * FROM ' . $tableName . ' WITH NO DATA';
            $this->db->exec($sql);
            $this->insertBatch($tmp_table, $fields, $values);
            $sql = 'INSERT INTO ' . $tableName . ' '
                 . '  SELECT * FROM ' . $tmp_table . ' '
                 . '  WHERE (idarchive, name) NOT IN '
                 . '    (SELECT idarchive, name FROM ' . $tableName . ')';
            $this->db->query($sql);

            $sql = 'DROP TABLE ' . $tmp_table;
            $this->db->exec($sql);
        }
        else {
            $this->insertBatch($tableName, $fields, $values);
        }
    }

    public function fetchAllBlob($table)
    {
        $this->confirmBlobTable($table);

        $valueCol = $this->prepareForBinary($table);
        $sql = 'SELECT *, '.$valueCol .' AS value_text FROM ' . $table;
        $rows = $this->db->fetchAll($sql);

        while (list($k, $row) = each($rows)) {
            $rows[$k]['value'] = $this->Generic->db2bin($row['value_text']);
            unset($rows[$k]['value_text']);
        }
        reset($rows);

        return $rows;
    }

    /**
     * Generate advisory lock name
     *
     * @param int            $idsite
     * @param Piwik_Period   $period
     * @param Piwik_Segment  $segment
     * @return string
     */
    protected function getProcessingLockName($idsite, $period, $segment)
    {
        $lockName = parent::getProcessingLockName($idsite, $period, $segment);
        $parts = explode('/', $lockName);
        $lockName = array_pop($parts);

        $segment_hash = $segment->isEmpty()
                        ? 0
                        : $this->md5_to_64bit($segment->getHash());

        $hash = $this->md5_to_64bit($lockName);

        $return = (float)$segment_hash
                + (float)$hash
                + $idsite
                + $period->getId()
                + strtotime($period->getDateStart()->toString('Y-m-d'))
                + strtotime($period->getDateEnd()->toString('Y-m-d'));
        $return = sprintf("%0.0f", $return);

        return $return;
    }

    /**
     *  64 bit number from md5 mash
     *
     *  Generates decimal number from md5 hash, divides it by
     *  64 bit integer and returns the remainder
     *  @param string   $md5
     *  @return float
     */
    protected function md5_to_64bit($md5)
    {
        $INT = 2147483647; # max. 32 bit integer on PHP
        $int_max = sprintf("%0.1f", $INT * $INT);
        $int = sprintf("%0.1f", $INT);
        
        $md5 = sprintf("%0.1f", hexdec($md5));
        while ($md5 > $int_max) { $md5 = $md5 / $int; }

        $quotient = $md5 / $int_max;
        $remainder = $md5 - (int)($quotient * $int_max);

        return sprintf("%0.0f", $remainder);
    }

    protected function prepareForBinary($table)
    {
        $this->Generic = Factory::getGeneric($this->db);
        $this->isBlobTable = $this->isBlob($table);
        $this->Generic->checkByteaOutput();

        $valueCol = $this->isBlobTable
                    ? $this->Generic->binaryColumn('value')
                    : ' value ' ;

        return $valueCol;
    }

    protected function binaryOutput($rows, $is_array = false)
    {
        if ($this->isBlobTable) {
            if ($is_array) {
                while (list($k, $row) = each($rows))  {
                    if (isset($row['value'])) {
                        $rows[$k]['value'] = $this->Generic->db2bin($row['value']);
                    }
                }
            }
            else {
                if (isset($rows['value'])) {
                    $rows['value'] = $this->Generic->db2bin($rows['value']);
                }
            }
        }

        return $rows;
    }

    protected function lockNameForNextIdarchive($table)
    {
        $hash = md5("loadNextIdArchive.$table" . Common::getSalt());

        $lockName = (float)$this->md5_to_64bit($hash);
        $lockName = sprintf("%0.0f", $lockName);

        return $lockName;
    }

    protected function insertBatch($tableName, $fields, $values)
    {
        $fieldList = '('.join(',', $fields).')';
        $params = Common::getSqlStringFieldsArray($values[0]);

        $sql_base = 'INSERT INTO ' . $tableName . $fieldList . ' VALUES ';
        $count = 0;
        $sql_parts = array();
        $bind      = array();
        while (list($k, $row) = each($values)) {
            $sql_parts[] = '(' . $params . ')';
            $bind        = array_merge($bind, array_values($row));
            ++$count;

            if ($count == 100) {
                $sql = $sql_base . implode(",\n", $sql_parts);
                $this->db->query($sql, $bind);
                $count = 0;
                $sql_parts = array();
                $bind      = array();
            }
        }
        
        if (count($sql_parts) > 0) {
            $sql = $sql_base . implode(",\n", $sql_parts);
            $this->db->query($sql, $bind);
        }
    }
}
