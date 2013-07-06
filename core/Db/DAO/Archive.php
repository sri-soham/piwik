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

/**
 *  @package Piwik
 *  @subpackage Piwik_Db
 */

# Class name is irrelevant for this class
# While instantiating, "archive" will be used, but there isn't
# any table with that name. 
# We do have 'archive_blob_2012_01', where 2012 is year and 01 is month
# We also have 'archive_numeric_2012_01'
# This is kind of a place holder for queries that have to be run on tables
# that begin with 'archive_'

class Piwik_Db_DAO_Archive extends Piwik_Db_DAO_Base
{
    public function __construct($db, $table)
    {
        parent::__construct($db, $table);
    }

    public function deleteByIdarchiveName($table, $idArchive, $name1, $name2)
    {
        $sql = 'DELETE FROM ' . $table . ' '
             . 'WHERE idarchive = ? '
             . "  AND (name = '$name1' OR name LIKE '$name2%')";
        $this->db->query($sql, array($idArchive));
    }

    public function deleteByDates($table, $idSites, $dates)
    {
        $sql_parts = $bind = array();
        foreach ($dates as $date) {
            $sql_parts[] = '(date1 <= ? AND ? <= date2)';
            $bind[] = $date;
            $bind[] = $date;
        }
        $sql_parts = implode(' OR ', $sql_parts);
        $sql = 'DELETE FROM ' . $table 
             . ' WHERE ( ' . $sql_parts . ' ) '
             . '   AND idsite IN ( ' . $idSites . ' )';
        $this->db->query($sql, $bind);
    }

    public function getIdarchiveByValueTS($table, $value1, $value2, $ts)
    {
        $sql = 'SELECT idarchive FROM ' . $table . ' '
             . "WHERE name LIKE 'done%' "
             . '  AND ( '
             . '    (value = ' . $value1 . ' AND ts_archived < ?) '
             . '    OR '
             . '    (value = ' . $value2 . ') '
             . '  ) ';
        $this->db->query($sql, array($ts));
    }

    public function deleteByIdarchive($tables, $idarchives)
    {
        foreach ($tables as $table) {
            $sql = 'DELETE FROM ' . $table .' WHERE idarchive IN ( '.implode(', ', $idarchives).' ) ';
            $this->db->query($sql);
        }
    }

    public function deleteByPeriodTS($tables, $period, $ts)
    {
        foreach ($tables as $table) {
            $sql = 'DELETE FROM ' . $table . ' '
                 . 'WHERE period = ? AND ts_archived < ?';
            $this->db->query($sql, array($period, $ts));
        }
    }

    public function getByIdarchiveName($table, $idarchive, $name)
    {
        $sql = 'SELECT value, ts_archived FROM ' . $table . ' '
             . 'WHERE idarchive = ? AND name = ?';
        return $this->db->fetchRow($sql, array($idarchive, $name));
    }

    public function getAllByIdarchiveNameLike($table, $idarchive, $name)
    {
        $nameEnd = strlen($name) + 2;
        $sql = "SELECT value, name FROM $table
                WHERE idarchive = ? AND 
                        (name = ? OR
                            (name LIKE ? AND
                            SUBSTRING(name, $nameEnd, 1) >= '0' AND
                            SUBSTRING(name, $nameEnd, 1) <= '9'
                            )
                        )";
        return $this->db->fetchAll($sql, array($idarchive, $name, $name.'%'));
    }

    public function getByIdsNames($table, $archiveIds, $fields)
    {
        $inNames = Piwik_Common::getSqlStringFieldsArray($fields);
        $sql = 'SELECT value, name, idarchive, idsite FROM ' . $table . ' '
             . "WHERE idarchive IN ($archiveIds) "
             . "  AND name IN ($inNames)";
        return $this->db->fetchAll($sql, $fields);
    }

    public function getIdsWithoutLaunching($table, $doneFlags, $idSites, $date1, $date2, $period)
    {
        $nameCondition = " (name IN ($doneFlags)) AND "
                        .'(value = ' . Piwik_ArchiveProcessing::DONE_OK 
                        .' OR value = ' . Piwik_ArchiveProcessing::DONE_OK_TEMPORARY 
                        .' ) ';
        $sql = 'SELECT idsite, MAX(idarchive) AS idarchive '
             . 'FROM ' . $table . ' '
             . 'WHERE date1 = ? '
             . '  AND date2 = ? '
             . '  AND period = ? '
             . '  AND ' . $nameCondition
             . '  AND idsite IN ( ' . implode(', ', $idSites) . ' ) '
             . 'GROUP BY idsite';
        return $this->db->fetchAll($sql, array($date1, $date2, $period));
    }

    // this is only for archive_numeric_* tables
    public function getForNumericDataTable($table, $ids, $names)
    {
        $inNames = Piwik_Common::getSqlStringFieldsArray($names);
        $sql = 'SELECT value, name, date1 AS start_date '
             . 'FROM ' . $table . ' '
             . 'WHERE idarchive IN ( ' . $ids . ' ) '
             . '  AND name IN ( ' . $inNames . ' ) '
             . 'ORDER BY date1, name';
        return $this->db->fetchAll($sql, $names);
    }

    public function loadNextIdarchive($table, $alias, $locked, $idsite, $date)
    {
        $dbLockName = "loadNextIdArchive.$table";
        $generic = Piwik_Db_Factory::getGeneric($this->db);

        if ($generic->getDbLock($dbLockName, $maxRetries = 30) === false) {
            throw new Exception("loadNextIdArchive: Cannot get named lock for table $table");
        }

        $sql = "INSERT INTO $table "
             . '   SELECT IFNULL(MAX(idarchive),0)+1 '
             . "        , '$locked' "
             . "        , $idsite "
             . "        , '$date' "
             . "        , '$date' "
             . '        , 0 '
             . "        , '$date' "
             . '        , 0 '
             . "   FROM $table AS $alias";
        $this->db->exec($sql);
        
        $generic->releaseDbLock($dbLockName);
    }

    public function getIdByName($table, $name)
    {
        $sql = 'SELECT idarchive FROM ' . $table . ' WHERE name = ? LIMIT 1';
        return $this->db->fetchOne($sql, array($name));
    }
    
    // this is for archive_numeric_* tables only
    public function isArchived($table, $done, $doneAll, $minDate, $idSite, $date1, $date2, $period)
    {
        $DONE_OK = Piwik_ArchiveProcessing::DONE_OK;
        $DONE_OK_TEMPORARY = Piwik_ArchiveProcessing::DONE_OK_TEMPORARY;

        $bind = array($idSite, $date1, $date2, $period);

        if ($done != $doneAll) {
            $sqlSegmentsFindArchiveAllPlugins = " OR (name = '$doneAll' AND value = $DONE_OK ) "
                                              . " OR (name = '$doneAll' AND value = $DONE_OK_TEMPORARY ) ";
        }
        else {
            $sqlSegmentsFindArchiveAllPlugins = '';
        }

        if ($minDate) {
            $timestampWhere = ' AND ts_archived >= ? ';
            $bind[] = $minDate;
        }
        else {
            $timestampWhere = '';
        }

        $startDate = $this->db->quoteIdentifier('startDate');
        $sql = "SELECT idarchive, value, name, date1 AS $startDate "
             . 'FROM ' . $table . ' '
             . 'WHERE idsite = ? '
             . '  AND date1 = ? '
             . '  AND date2 = ? '
             . '  AND period = ? '
             . "  AND (   (name='$done' AND value=$DONE_OK) "
             . "       OR (name='$done' AND value=$DONE_OK_TEMPORARY) "
             . "       $sqlSegmentsFindArchiveAllPlugins "
             . "       OR name = 'nb_visits' "
             . '    ) '
             . $timestampWhere
             . ' ORDER BY idarchive DESC';
        return $this->db->fetchAll($sql, $bind);
    }

    public function createPartitionTable($tableName, $generatedTableName)
    {
        $sql = $this->getPartitionTableSql($tableName, $generatedTableName);
        $this->db->query($sql);
    }

    /**
     * Get an advisory lock
     *
     * @param int            $idsite
     * @param Piwik_Period   $period
     * @param Piwik_Segment  $segment
     * @return bool  True if lock acquired; false otherwise
     */
    public function getProcessingLock($idsite, $period, $segment)
    {
        $lockName = $this->getProcessingLockName($idsite, $period, $segment);
        $date = $period->getDateStart()->toString('Y-m-d')
                .','
                .$period->getDateEnd()->toString('Y-m-d');

        $generic = Piwik_Db_Factory::getGeneric($this->db);

        return $generic->getDbLock($lockName);
    }

    /**
     * Release an advisory lock
     *
     * @param int            $idsite
     * @param Piwik_Period   $period
     * @param Piwik_Segment  $segment
     * @return bool True if lock released; false otherwise
     */
    public function releaseProcessingLock($idsite, $period, $segment)
    {
        $lockName = $this->getProcessingLockName($idsite, $period, $segment);
        $date = $period->getDateStart()->toString('Y-m-d')
                .','
                .$period->getDateEnd()->toString('Y-m-d');
        $generic = Piwik_Db_Factory::getGeneric($this->db);

        return $generic->releaseDbLock($lockName);
    }

    public function insertIgnoreBatch($tableName, $fields, $values, $ignoreWhenDuplicate)
    {
        $Generic = Piwik_Db_Factory::getGeneric($this->db);
        $Generic->insertIgnoreBatch($tableName, $fields, $values, $ignoreWhenDuplicate);
    }

    public function insertRecord($tableName, $bindArray)
    {
        $values = Piwik_Common::getSqlStringFieldsArray($bindArray);
        $sql = 'INSERT IGNORE INTO ' . $tableName . '( '. implode(', ', array_keys($bindArray)) . ')'
             . ' VALUES ( ' . $values . ' ) ';
        $this->db->query($sql, array_values($bindArray));
    }

    public function fetchAllBlob($table)
    {
        $this->confirmBlobTable($table);
        $sql = 'SELECT * FROM ' . $table;

        return $this->db->fetchAll($sql);
    }

    protected function getPartitionTableSql($tableName, $generatedTableName)
    {
        $config = Piwik_Config::getInstance();
        $prefix = $config->database['tables_prefix'];
        $sql = Piwik::getTableCreateSql($tableName);
        $sql = str_replace($prefix . $tableName, $generatedTableName, $sql);
        $sql = str_replace('CREATE TABLE', 'CREATE TABLE IF NOT EXISTS', $sql);

        return $sql;
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
        $config = Piwik_Config::getInstance();

        $lockName = 'piwik.'
            . $config->database['dbname'] . '.'
            . $config->database['tables_prefix'] . '/'
            . $idsite . '/'
            . (!$segment->isEmpty() ? $segment->getHash().'/' : '' )
            . $period->getId() . '/'
            . $period->getDateStart()->toString('Y-m-d') . ','
            . $period->getDateEnd()->toString('Y-m-d');
        $return = $lockName .'/'. md5($lockName . Piwik_Common::getSalt());
    
        return $return;
    }

    protected function isBlob($table)
    {
        $pos = strpos($table, 'archive_blob');
        return $pos !== false;
    }

    protected function confirmBlobTable($table)
    {
        if (!$this->isBlob($table)) {
            throw new Exception('Table is ' . $table . '. Only  blob tables are allowed');
        }
    }
}
