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
namespace Piwik\Db\DAO\Pgsql;

use Piwik\Common;
use Piwik\Db\Factory;
use Piwik\Plugins\PrivacyManager\LogDataPurger;
use Piwik\Piwik;

/**
 * @package Piwik
 * @subpackage Piwik_Db
 */

class LogAction extends \Piwik\Db\DAO\Mysql\LogAction
{ 
    const TEMP_TABLE_NAME = 'tmp_log_actions_to_keep';

    private $generic;

    public function __construct($db, $table)
    {
        parent::__construct($db, $table);
    }

    // to overcome the "could not determine datatype of parameter $4 error"
    // for the bound variables like '*\_)\%'
    // sample sql that can generate the error
    // SELECT CONCAT('%', ?, '%') AS name;
    // array('*\_)\%') or any string like array('piwik')
    public function sqlIdactionFromSegment($matchType, $actionType)
    {
        $sql = 'SELECT idaction FROM ' . $this->table . ' WHERE ';
        switch ($matchType) {
            case '=@':
                $sql .= "(name LIKE '%' || ? || '%' AND type = $actionType )";
                break;
            case '!@':
                $sql .= "(name NOT LIKE '%' || ? || '%' AND type = $actionType )";
                break;
            default:
                throw new \Exception("This match type is not available for action-segments.");
                break;
        }

        return $sql;
    }

    /**
     *  add record
     *
     *  Adds a record to the log_action table and returns the id of the
     *  the inserted row.
     *
     *  @param string $name
     *  @param string $type
     *  @param int    $urlPrefix
     *  @returns int
     */
    public function add($name, $type, $urlPrefix)
    {
        $sql = 'INSERT INTO ' . $this->table . ' (name, hash, type, url_prefix) '
             . 'VALUES (?, ?, ?, ?)';
        $this->db->query($sql, array($name, Common::getCrc32($name), $type, $urlPrefix));

        return $this->db->lastInsertId($this->table.'_idaction');
    }

    /**
     *  delete Unused actions
     *
     *  Deletes the data from log_action table based on the temporary table
     */
    public function deleteUnusedActions()
    {
        $tempTable = Common::prefixTable(self::TEMP_TABLE_NAME);
        $sql = 'DELETE FROM ' . $this->table . ' AS la WHERE NOT EXISTS '
             . '(SELECT * FROM ' . $tempTable. ' AS tmp WHERE tmp.idaction = la.idaction)';
        $this->db->query($sql);
    }

    public function purgeUnused()
    {
        // get current max visit ID in log tables w/ idaction references.
        $maxIds = $this->getMaxIdsInLogTables();
        $this->generic = Factory::getGeneric($this->db);
        $this->createTempTable();

        // do large insert (inserting everything before maxIds) w/o locking tables...
        $this->insertActionsToKeep($maxIds, $deleteOlderThanMax = true);

        // ... then do small insert w/ locked tables to minimize the amount of time tables are locked.
        $this->generic->beginTransaction();
        $this->lockLogTables($this->generic);
        $this->insertActionsToKeep($maxIds, $deleteOlderThanMax = false);
        
        // delete before unlocking tables so there's no chance a new log row that references an
        // unused action will be inserted.
        $this->deleteDuplicatesFromTempTable();
        $this->deleteUnusedActions();
        // unlock the log tables
        $this->generic->commit();
        $this->generic = null;
    }

    protected function insertActionsToKeep($maxIds, $olderThan = true)
    {
        $tempTable = Common::prefixTable(self::TEMP_TABLE_NAME);
        $idColumns = $this->getTableIdColumns();
        foreach ($this->getIdActionColumns() as $table => $columns) {
            $idCol = $idColumns[$table];
            foreach ($columns as $col) {
                $select = "SELECT $col from " . Common::prefixTable($table) . " WHERE $idCol >= ? AND $idCol < ?";
                $sql = "INSERT INTO $tempTable $select";
                if ($olderThan) {
                    $start = 0;
                    $finish = $maxIds[$table];
                }
                else {
                    $start = $maxIds[$table];
                    $finish = $this->generic->getMax(Common::prefixTable($table), $idCol);
                }
                $this->generic->segmentedQuery($sql, $start, $finish, LogDataPurger::$selectSegmentSize);
            }
        }

        // allow code to be executed after data is inserted. for concurrency testing purposes.
        if ($olderThan) {
            /**
             * @ignore
             */
            Piwik::postEvent("LogDataPurger.actionsToKeepInserted.olderThan");
        }
        else {
            /**
             * @ignore
             */
            Piwik::postEvent("LogDataPurger.actionsToKeepInserted.newerThan");
        }
    }

    protected function lockLogTables($generic)
    {
        $generic->lockTables(
            $readLocks = Common::prefixTables('log_conversion',
                                                    'log_link_visit_action',
                                                    'log_visit',
                                                    'log_conversion_item'
                                                    ),
            $writeLocks = Common::prefixTable('log_action')
        );
    }

    /**
     *  create temporary table
     *
     *  Creates the temporary table; idaction is not the primary key as it is
     *  in the mysql version. Postgres doesn't support INSERT IGNORE. To get
     *  around that, all idactions values are added to the temporary table
     *  and the duplicates are deleted before the call to "deleteUnusedActions".
     *  deleteDuplicatesFromTempTable does the job of removing duplicaets.
     */
    protected function createTempTable()
    {
        $sql = 'CREATE TEMPORARY TABLE ' . Common::prefixTable(self::TEMP_TABLE_NAME) . '( '
              .'  idaction INT'
              .' );';
        $this->db->query($sql);
    }

    protected function deleteDuplicatesFromTempTable()
    {
        $tempTempTable = Common::prefixTable(self::TEMP_TABLE_NAME . '_tmp');
        $tempTable = Common::prefixTable(self::TEMP_TABLE_NAME);
        $sql = "CREATE TEMPORARY TABLE $tempTempTable AS SELECT idaction FROM $tempTable GROUP BY idaction";
        $this->db->query($sql);
        $this->db->query("TRUNCATE TABLE $tempTable");
        $this->db->query("INSERT INTO $tempTable SELECT idaction FROM $tempTempTable");
        $this->db->query("DROP TABLE $tempTempTable");
    }
} 
