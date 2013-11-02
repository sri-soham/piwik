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
use Piwik\Db\DAO\Base;
use Piwik\Db\Factory;
use Piwik\Plugins\PrivacyManager\LogDataPurger;
use Piwik\Tracker\Action;
use Piwik\Piwik;
use Piwik\Db;

/**
 * @package Piwik
 * @subpackage Piwik_Db
 */

class LogAction extends Base
{ 
    const TEMP_TABLE_NAME = 'tmp_log_actions_to_keep';

    public function __construct($db, $table)
    {
        parent::__construct($db, $table);
    }

    public function sqlIdactionFromSegment($matchType, $actionType)
    {
        $sql = 'SELECT idaction FROM ' . $this->table . ' WHERE ';
        switch ($matchType) {
            case '=@':
                $sql .= "(name LIKE CONCAT('%', ?, '%') AND type = $actionType )";
                break;
            case '!@':
                $sql .= "(name NOT LIKE CONCAT('%', ?, '%') AND type = $actionType )";
                break;
            default:
                throw new \Exception("This match type is not available for action-segments.");
                break;
        }

        return $sql;
    }

    public function getIdaction($name, $type)
    {
        $sql = $this->sqlActionId();
        $bind = array(Common::getCrc32($name), $name, $type);

        $row = $this->db->fetchOne($sql, $bind);
        return $row;
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

        return $this->db->lastInsertId();
    }

    public function queryIdsAction(&$actionsNameAndType) {
        $sql = $this->sqlActionId();
        $bind = array();
        $i = 0;
        foreach ($actionsNameAndType as $index => &$actionNameType) {
            list($name, $type, $urlPrefix) = $actionNameType;
            if (empty($name)) {
                continue;
            }
            if ($i > 0) {
                $sql .= ' OR (hash = ? AND name = ? AND type = ? )';
            }
            $bind[] = Common::getCrc32($name);
            $bind[] = $name;
            $bind[] = $type;
            ++$i;
        }

        // Case URL & Title are empty
        if (empty($bind)) {
            return false;
        }

        $actionIds = $this->db->fetchAll($sql, $bind);
        return $actionIds;
    }

    /**
     *  delete Unused actions
     *
     *  Deletes the data from log_action table based on the temporary table
     */
    public function deleteUnusedActions()
    {
        $tempTable = Common::prefixTable(self::TEMP_TABLE_NAME);
        $sql = "DELETE LOW_PRIORITY QUICK IGNORE {$this->table} "
              ."FROM {$this->table} "
              .'LEFT OUTER JOIN ' . $tempTable . ' AS tmp '
              ."    ON tmp.idaction = {$this->table}.idaction "
              .'WHERE tmp.idaction IS NULL';
        $this->db->query($sql);
    }

    public function getIdactionByName($name)
    {
        $sql = 'SELECT idaction FROM ' . $this->table . ' WHERE name = ?';
        return $this->db->fetchOne($sql, array($name));
    }

    public function getCountByIdaction($idaction)
    {
        $sql = 'SELECT COUNT(*) FROM ' . $this->table . ' WHERE idaction = ?';
        return $this->db->fetchOne($sql, array($idaction));
    }

    public function purgeUnused()
    {
        // get current max visit ID in log tables w/ idaction references.
        $maxIds = $this->getMaxIdsInLogTables();
        $generic = Factory::getGeneric($this->db);
        $this->createTempTable();

        // do large insert (inserting everything before maxIds) w/o locking tables...
        $this->insertActionsToKeep($maxIds, $deleteOlderThanMax = true);

        // ... then do small insert w/ locked tables to minimize the amount of time tables are locked.
        $this->lockLogTables($generic);
        $this->insertActionsToKeep($maxIds, $deleteOlderThanMax = false);
        
        // ... then do small insert w/ locked tables to minimize the amount of time tables are locked.
        // delete before unlocking tables so there's no chance a new log row that references an
        // unused action will be inserted.
        $this->deleteUnusedActions();
        // unlock the log tables
        $generic->unlockAllTables();
    }

    protected function getIdActionColumns()
    {
        return array(
            'log_link_visit_action' => array( 'idaction_url',
                                              'idaction_url_ref',
                                              'idaction_name',
                                              'idaction_name_ref' ),
                                              
            'log_conversion' => array( 'idaction_url' ),
            
            'log_visit' => array( 'visit_exit_idaction_url',
                                  'visit_exit_idaction_name',
                                  'visit_entry_idaction_url',
                                  'visit_entry_idaction_name' ),
                                  
            'log_conversion_item' => array( 'idaction_sku',
                                            'idaction_name',
                                            'idaction_category',
                                            'idaction_category2',
                                            'idaction_category3',
                                            'idaction_category4',
                                            'idaction_category5' )
        );
    }

    protected function sqlActionId()
    {
        $sql = 'SELECT idaction, type, name '
              .'FROM ' . $this->table . ' '
              .'WHERE ( hash = ? AND name = ? AND type = ? ) ';
        return $sql;
    }

    protected function insertActionsToKeep($maxIds, $olderThan = true)
    {
        $Generic = Factory::getGeneric($this->db);

        $tempTable = Common::prefixTable(self::TEMP_TABLE_NAME);

        $idColumns = $this->getTableIdColumns();
        foreach ($this->getIdActionColumns() as $table => $columns) {
            $idCol = $idColumns[$table];
            foreach ($columns as $col) {
                $select = "SELECT $col from " . Common::prefixTable($table) . " WHERE $idCol >= ? AND $idCol < ?";
                $sql = "INSERT IGNORE INTO $tempTable $select";

                if ($olderThan) {
                    $start  = 0;
                    $finish = $maxIds[$table];
                }
                else {
                    $start  = $maxIds[$table];
                    $finish = $Generic->getMax(Common::prefixTable($table), $idCol);
                }

                $Generic->segmentedQuery($sql, $start, $finish, LogDataPurger::$selectSegmentSize);
            }
        }

        // allow code to be executed after data is inserted. for concurrency testing purposes.
        if ($olderThan) {
            /**
             * @ignore
             */
            Piwik::postEvent("LogDataPurger.ActionsToKeepInserted.olderThan");
        }
        else {
            /**
             * @ignore
             */
            Piwik::postEvent("LogDataPurger.ActionsToKeepInserted.newerThan");
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

    protected function createTempTable()
    {
        $sql = 'CREATE TEMPORARY TABLE ' . Common::prefixTable(self::TEMP_TABLE_NAME) . '( '
              .'  idaction INT, '
              .'  PRIMARY KEY(idaction) '
              .' );';
        $this->db->query($sql);
    }

    protected function getTableIdColumns()
    {
        return array(
            'log_link_visit_action' => 'idlink_va',
            'log_conversion'        => 'idvisit',
            'log_visit'             => 'idvisit',
            'log_conversion_item'   => 'idvisit'
        );
    }

    protected function getMaxIdsInLogTables()
    {
        $Generic = Factory::getGeneric($this->db);

        $result = array();
        foreach ($this->getTableIdColumns() as $table => $col) {
            $result[$table] = $Generic->getMax(Common::prefixTable($table), $col);
        }

        return $result;
    }
} 
