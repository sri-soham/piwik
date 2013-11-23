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
namespace Piwik\DataAccess;

use Exception;
use Piwik\ArchiveProcessor\Rules;
use Piwik\ArchiveProcessor;
use Piwik\Common;
use Piwik\Config;
use Piwik\Db;
use Piwik\Db\BatchInsert;

use Piwik\Log;
use Piwik\Period;
use Piwik\Segment;
use Piwik\SettingsPiwik;

/**
 * This class is used to create a new Archive.
 * An Archive is a set of reports (numeric and data tables).
 * New data can be inserted in the archive with insertRecord/insertBulkRecords
 */
class ArchiveWriter
{
    const PREFIX_SQL_LOCK = "locked_";

    protected $fields = array('idarchive',
                              'idsite',
                              'date1',
                              'date2',
                              'period',
                              'ts_archived',
                              'name',
                              'value');

    protected $Archive;

    public function __construct($idSite, Segment $segment, Period $period, $requestedPlugin, $isArchiveTemporary)
    {
        $this->idArchive = false;
        $this->idSite = $idSite;
        $this->segment = $segment;
        $this->period = $period;
        $this->doneFlag = Rules::getDoneStringFlagFor($segment, $period->getLabel(), $requestedPlugin);
        $this->isArchiveTemporary = $isArchiveTemporary;

        $this->dateStart = $this->period->getDateStart();

        $this->Archive = \Piwik\Db\Factory::getDAO('archive');
    }

    protected function getArchiveLockName()
    {
        $numericTable = $this->getTableNumeric();
        $dbLockName = "allocateNewArchiveId.$numericTable";
        return $dbLockName;
    }

    /**
     * @return array
     * @throws \Exception
     */
    protected function acquireArchiveTableLock()
    {
        $dbLockName = $this->getArchiveLockName();
        if (Db::getDbLock($dbLockName, $maxRetries = 30) === false) {
            throw new Exception("allocateNewArchiveId: Cannot get named lock for table $numericTable.");
        }
    }

    protected function releaseArchiveTableLock()
    {
        $dbLockName = $this->getArchiveLockName();
        Db::releaseDbLock($dbLockName);
    }

    public function getIdArchive()
    {
        if ($this->idArchive === false) {
            throw new Exception("Must call allocateNewArchiveId() first");
        }
        return $this->idArchive;
    }

    public function initNewArchive()
    {
        $this->acquireLock();
        $this->allocateNewArchiveId();
        $this->logArchiveStatusAsIncomplete();
    }

    protected function acquireLock()
    {
        $result = $this->Archive->getProcessingLock($this->idSite, $this->period, $this->segment);
        if (!$result) {
            Log::debug("SELECT GET_LOCK failed to acquire lock. Proceeding anyway.");
        }
    }

    protected function allocateNewArchiveId()
    {
        $this->idArchive = $this->insertNewArchiveId();
        return $this->idArchive;
    }

    protected function insertNewArchiveId()
    {
        $numericTable = $this->getTableNumeric();
        $locked = self::PREFIX_SQL_LOCK . Common::generateUniqId();
        $date = date("Y-m-d H:i:s");
        $this->Archive->loadNextIdarchive($numericTable, "tb1", $locked, $this->idSite, $date);
        $id = $this->Archive->getIdByName($numericTable, $locked);

        return $id;
    }

    protected function logArchiveStatusAsIncomplete()
    {
        $statusWhileProcessing = ArchiveProcessor::DONE_ERROR;
        $this->insertRecord($this->doneFlag, $statusWhileProcessing);
    }

    protected function getArchiveProcessorLockName()
    {
        return self::makeLockName($this->idSite, $this->period, $this->segment);
    }

    protected static function makeLockName($idsite, Period $period, Segment $segment)
    {
        $config = Config::getInstance();

        $lockName = 'piwik.'
            . $config->database['dbname'] . '.'
            . $config->database['tables_prefix'] . '/'
            . $idsite . '/'
            . (!$segment->isEmpty() ? $segment->getHash() . '/' : '')
            . $period->getId() . '/'
            . $period->getDateStart()->toString('Y-m-d') . ','
            . $period->getDateEnd()->toString('Y-m-d');
        return $lockName . '/' . md5($lockName . SettingsPiwik::getSalt());
    }

    public function finalizeArchive()
    {
        $this->deletePreviousArchiveStatus();
        $this->logArchiveStatusAsFinal();
        $this->releaseArchiveProcessorLock();
    }

    protected function deletePreviousArchiveStatus()
    {
        $this->Archive->deletePreviousArchiveStatus(
            $this->getTableNumeric(),
            $this->getIdArchive(),
            $this->doneFlag,
            self::PREFIX_SQL_LOCK
        );
    }

    protected function logArchiveStatusAsFinal()
    {
        $status = ArchiveProcessor::DONE_OK;
        if ($this->isArchiveTemporary) {
            $status = ArchiveProcessor::DONE_OK_TEMPORARY;
        }
        $this->insertRecord($this->doneFlag, $status);
    }

    protected function releaseArchiveProcessorLock()
    {
        return $this->Archive->releaseProcessingLock($this->idSite, $this->period, $this->segment);
    }

    public function insertBulkRecords($records)
    {
        // Using standard plain INSERT if there is only one record to insert
        if ($DEBUG_DO_NOT_USE_BULK_INSERT = false
            || count($records) == 1
        ) {
            foreach ($records as $record) {
                $this->insertRecord($record[0], $record[1]);
            }
            return true;
        }
        $bindSql = $this->getInsertRecordBindNamed();
        $values = array();

        $valueSeen = false;
        foreach ($records as $record) {
            // don't record zero
            if (empty($record[1])) continue;

            $bind = $bindSql;
            $bind['name'] = $record[0]; // name
            $bind['value'] = $record[1]; // value
            $values[] = $bind;

            $valueSeen = $record[1];
        }
        if (empty($values)) return true;

        $tableName = $this->getTableNameToInsert($valueSeen);
        $this->Archive->insertIgnoreBatch($tableName, $this->getInsertFields(), $values, true);
        return true;
    }

    /**
     * Inserts a record in the right table (either NUMERIC or BLOB)
     *
     * @param string $name
     * @param mixed $value
     *
     * @return bool
     */
    public function insertRecord($name, $value)
    {
        if ($this->isRecordZero($value)) {
            return false;
        }

        $tableName = $this->getTableNameToInsert($value);

        // duplicate idarchives are Ignored, see http://dev.piwik.org/trac/ticket/987
        $bind = $this->getInsertRecordBindNamed();
        $bind['name'] = $name;
        $bind['value'] = $value;
        $this->Archive->insertRecord($tableName, $bind);
        return true;
    }

    protected function getInsertRecordBind()
    {
        return array($this->getIdArchive(),
                     $this->idSite,
                     $this->dateStart->toString('Y-m-d'),
                     $this->period->getDateEnd()->toString('Y-m-d'),
                     $this->period->getId(),
                     date("Y-m-d H:i:s"));
    }

    protected function getInsertRecordBindNamed()
    {
        return array('idarchive' => $this->getIdArchive(),
                     'idsite'    => $this->idSite,
                     'date1'     => $this->dateStart->toString('Y-m-d'),
                     'date2'     => $this->period->getDateEnd()->toString('Y-m-d'),
                     'period'    => $this->period->getId(),
                     'ts_archived' => date('Y-m-d H:i:s')
              );
    }

    protected function getTableNameToInsert($value)
    {
        if (is_numeric($value)) {
            return $this->getTableNumeric();
        }
        return ArchiveTableCreator::getBlobTable($this->dateStart);
    }

    protected function getTableNumeric()
    {
        return ArchiveTableCreator::getNumericTable($this->dateStart);
    }

    protected function getInsertFields()
    {
        return $this->fields;
    }

    protected function isRecordZero($value)
    {
        return ($value === '0' || $value === false || $value === 0 || $value === 0.0);
    }
}
