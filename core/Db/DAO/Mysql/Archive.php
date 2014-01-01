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
namespace Piwik\Db\DAO\Mysql;

use Exception;
use Piwik\ArchiveProcessor;
use Piwik\Common;
use Piwik\Config;
use Piwik\Date;
use Piwik\DataAccess\ArchiveSelector;
use Piwik\DataAccess\ArchiveTableCreator;
use Piwik\Db\Factory;
use Piwik\DbHelper;
use Piwik\Db\DAO\Base;
use Piwik\Piwik;
use Piwik\Log;
use Piwik\SettingsPiwik;

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

class Archive extends Base
{
    public function __construct($db, $table)
    {
        parent::__construct($db, $table);
    }

    public function deletePreviousArchiveStatus($table, $idArchive, $name1, $name2)
    {
        $dbLockName = "allocateNewArchiveId.$table";
        $generic = Factory::getGeneric($this->db);

        if ($generic->getDbLock($dbLockName, $maxRetries = 30) === false) {
            throw new Exception("loadNextIdArchive: Cannot get named lock for table $table");
        }
        $this->deleteByIdarchiveName($table, $idArchive, $name1, $name2);

        $generic->releaseDbLock($dbLockName);
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
             . '   AND idsite IN ( ' . implode(',', $idSites) . ' )';
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

    public function deleteByArchiveIds(Date $date, $idArchivesToDelete)
    {
        $query = "DELETE FROM %s WHERE idarchive IN (" . implode(',', $idArchivesToDelete) . ")";

        $this->db->query(sprintf($query, ArchiveTableCreator::getNumericTable($date)));
        try {
            $this->db->query(sprintf($query, ArchiveTableCreator::getBlobTable($date)));
        } catch (Exception $e) {
            // Individual blob tables could be missing
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
        $inNames = Common::getSqlStringFieldsArray($fields);
        $sql = 'SELECT value, name, idarchive, idsite FROM ' . $table . ' '
             . "WHERE idarchive IN ($archiveIds) "
             . "  AND name IN ($inNames)";
        return $this->db->fetchAll($sql, $fields);
    }

    public function getIdsWithoutLaunching($table, $doneFlags, $idSites, $date1, $date2, $period)
    {
        $nameCondition = " (name IN ($doneFlags)) AND "
                        .'(value = ' . ArchiveProcessor::DONE_OK 
                        .' OR value = ' . ArchiveProcessor::DONE_OK_TEMPORARY 
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
        $inNames = Common::getSqlStringFieldsArray($names);
        $startDate = $this->db->quoteIdentifier('startDate');
        $sql = "SELECT value, name, date1 AS $startDate "
             . 'FROM ' . $table . ' '
             . 'WHERE idarchive IN ( ' . $ids . ' ) '
             . '  AND name IN ( ' . $inNames . ' ) '
             . 'ORDER BY date1, name';
        return $this->db->fetchAll($sql, $names);
    }

    public function loadNextIdarchive($table, $alias, $locked, $idsite, $date)
    {
        $dbLockName = "allocateNewArchiveId.$table";
        $generic = Factory::getGeneric($this->db);

        if ($generic->getDbLock($dbLockName, $maxRetries = 30) === false) {
            throw new Exception("loadNextIdArchive: Cannot get named lock for table $table");
        }

        $idsite = (int)$idsite;
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
   
    // used for *_numeric tables
    public function getArchiveIdAndVisits($table, \Piwik\Site $site,
                                          \Piwik\Period $period, $minDatetime,
                                          $sqlWhereArchiveName) {
        $bind = array($site->getId(),
                      $period->getDateStart()->toString('Y-m-d'),
                      $period->getDateEnd()->toString('Y-m-d'),
                      $period->getId()
                );
        if ($minDatetime) {
            $timestampWhere = ' AND ts_archived >= ?';
            $bind[] = Date::factory($minDatetime)->getDatetime();
        }
        else {
            $timestampWhere = '';
        }

        $startDate = $this->db->quoteIdentifier('startDate');
        $sql = "SELECT idarchive, value, name, date1 AS $startDate
                FROM $table
                WHERE idsite = ?
                  AND date1 = ?
                  AND date2 = ?
                  AND period = ?
                  AND ( ($sqlWhereArchiveName)
                        OR name = '" . ArchiveSelector::NB_VISITS_RECORD_LOOKED_UP ."'
                        OR name = '" . ArchiveSelector::NB_VISITS_CONVERTED_RECORD_LOOKED_UP . "'
                     )
                  $timestampWhere
                ORDER BY idarchive DESC";
        return $this->db->fetchAll($sql, $bind);
    }

    public function getArchiveIds($siteIds, $monthToPeriods, $nameCondition) {
        $sql = "SELECT idsite, name, date1, date2, MAX(idarchive) as idarchive
                FROM %s
                WHERE period = ?
                  AND %s
                  AND {$nameCondition}
                  AND idsite IN (" . implode(',', $siteIds) . ")
                GROUP BY idsite, name, date1, date2";

        // for every month within the archive query, select from numeric table
        $result = array();
        foreach ($monthToPeriods as $table => $periods) {
            $firstPeriod = reset($periods);

            // if looking for a range archive. NOTE: we assume there's only one period if its a range.
            $bind = array($firstPeriod->getId());
            if ($firstPeriod instanceof Range) {
                $dateCondition = "date1 = ? AND date2 = ?";
                $bind[] = $firstPeriod->getDateStart()->toString('Y-m-d');
                $bind[] = $firstPeriod->getDateEnd()->toString('Y-m-d');
            } else { // if looking for a normal period
                $dateStrs = array();
                foreach ($periods as $period) {
                    $dateStrs[] = $period->getDateStart()->toString('Y-m-d');
                }

                $dateCondition = "date1 IN ('" . implode("','", $dateStrs) . "')";
            }

            $sql = sprintf($sql, $table, $dateCondition);

            // get the archive IDs
            foreach ($this->db->fetchAll($sql, $bind) as $row) {
                $archiveName = $row['name'];

                //FIXMEA duplicate with Archive.php
                $dateStr = $row['date1'] . "," . $row['date2'];

                $result[$archiveName][$dateStr][] = $row['idarchive'];
            }
        }

        return $result;
    }

    /**
     * Queries and returns archive data using a set of archive IDs.
     *
     * @param array $archiveIds The IDs of the archives to get data from.
     * @param array $recordNames The names of the data to retrieve (ie, nb_visits, nb_actions, etc.)
     * @param string $archiveDataType The archive data type (either, 'blob' or 'numeric').
     * @param bool $loadAllSubtables Whether to pre-load all subtables
     * @throws Exception
     * @return array
     */
    public function getArchiveData($archiveIds, $recordNames, $archiveDataType, $loadAllSubtables)
    {
        // create the SQL to select archive data
        $inNames = Common::getSqlStringFieldsArray($recordNames);
        if ($loadAllSubtables) {
            $name = reset($recordNames);

            // select blobs w/ name like "$name_[0-9]+" w/o using RLIKE
            $nameEnd = strlen($name) + 2;
            $whereNameIs = "(name = ?
                            OR (name LIKE ?
                                 AND SUBSTRING(name, $nameEnd, 1) >= '0'
                                 AND SUBSTRING(name, $nameEnd, 1) <= '9') )";
            $bind = array($name, $name . '%');
        } else {
            $whereNameIs = "name IN ($inNames)";
            $bind = array_values($recordNames);
        }

        $getValuesSql = "SELECT value, name, idsite, date1, date2, ts_archived
                         FROM %s
                         WHERE idarchive IN (%s)
                           AND " . $whereNameIs;

        // get data from every table we're querying
        $rows = array();
        foreach ($archiveIds as $period => $ids) {
            if (empty($ids)) {
                throw new Exception("Unexpected: id archive not found for period '$period' '");
            }
            // $period = "2009-01-04,2009-01-04",
            $date = Date::factory(substr($period, 0, 10));
            if ($archiveDataType == 'numeric') {
                $table = ArchiveTableCreator::getNumericTable($date);
            } else {
                $table = ArchiveTableCreator::getBlobTable($date);
            }
            $sql = sprintf($getValuesSql, $table, implode(',', $ids));
            $dataRows = $this->db->fetchAll($sql, $bind);
            foreach ($dataRows as $row) {
                $rows[] = $row;
            }
        }

        return $rows;
    }

    public function deleteByPeriodRange(Date $date)
    {
        $query = "DELETE FROM %s WHERE period = ? AND ts_archived < ?";

        $yesterday = Date::factory('yesterday')->getDateTime();
        $bind = array(Piwik::$idPeriods['range'], $yesterday);
        $numericTable = ArchiveTableCreator::getNumericTable($date);
        $this->db->query(sprintf($query, $numericTable), $bind);
        Log::debug("Purging Custom Range archives: done [ purged archives older than $yesterday from $numericTable / blob ]");
        try {
            $this->db->query(sprintf($query, ArchiveTableCreator::getBlobTable($date)), $bind);
        } catch (Exception $e) {
            // Individual blob tables could be missing
        }
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

        $generic = Factory::getGeneric($this->db);

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
        $generic = Factory::getGeneric($this->db);

        return $generic->releaseDbLock($lockName);
    }

    public function insertIgnoreBatch($tableName, $fields, $values, $ignoreWhenDuplicate)
    {
        $Generic = Factory::getGeneric($this->db);
        $Generic->insertIgnoreBatch($tableName, $fields, $values, $ignoreWhenDuplicate);
    }

    public function insertRecord($tableName, $bindArray)
    {
        $values = Common::getSqlStringFieldsArray($bindArray);
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

    protected function deleteByIdarchiveName($table, $idArchive, $name1, $name2)
    {
        $sql = 'DELETE FROM ' . $table . ' '
             . 'WHERE idarchive = ? '
             . "  AND (name = '$name1' OR name LIKE '$name2%')";
        $this->db->query($sql, array($idArchive));
    }

    protected function getPartitionTableSql($tableName, $generatedTableName)
    {
        $config = Config::getInstance();
        $prefix = $config->database['tables_prefix'];
        $sql = DbHelper::getTableCreateSql($tableName);
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
        $config = Config::getInstance();

        $lockName = 'piwik.'
            . $config->database['dbname'] . '.'
            . $config->database['tables_prefix'] . '/'
            . $idsite . '/'
            . (!$segment->isEmpty() ? $segment->getHash().'/' : '' )
            . $period->getId() . '/'
            . $period->getDateStart()->toString('Y-m-d') . ','
            . $period->getDateEnd()->toString('Y-m-d');
        $return = $lockName .'/'. md5($lockName . SettingsPiwik::getSalt());
    
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
