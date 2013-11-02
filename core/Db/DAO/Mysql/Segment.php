<?php
/**
 * Piwik - Open source web analytics
 $*
 * @link http://piwik.org
 * @license http://www.gnu.org/licenses/gpl-3.0.html GPL v3 or later
 *
 * @category Piwik
 * @package Piwik
 */
namespace Piwik\Db\DAO\Mysql;

use Piwik\Piwik;
use Piwik\Db\DAO\Base;

/**
 * @package Piwik
 * @subpackage Piwik_Db
 */

class Segment extends Base
{
    public function __construct($db, $table)
    {
        parent::__construct($db, $table);
    }

    public function deleteByIdsegment($idSegment)
    {
        $this->db->delete($this->table, 'idsegment = ' . $idSegment);
    }

    public function updateByIdsegment($bind, $idSegment)
    {
        $this->db->update($this->table, $bind, 'idsegment='.$idSegment);
    }

    public function add($bind)
    {
        $this->db->insert($this->table, $bind);
        return $this->db->lastInsertId();
    }

    public function getByIdsegment($idSegment)
    {
        $sql = "SELECT * FROM {$this->table} WHERE idsegment = ?";
        return $this->db->fetchRow($sql, array($idSegment));
    }

    public function getAll($idSite, $login, $returnOnlyAutoArchived)
    {
        $bind = array();

        // Build basic segment filtering
        $whereIdSite = '';
        if (!empty($idSite)) {
            $whereIdSite = 'enable_only_idsite = ? OR ';
            $bind[] = $idSite;
        }

        $bind[] = $login;

        $extraWhere = '';
        if ($returnOnlyAutoArchived) {
            $extraWhere = ' AND auto_archive = 1';
        }

        $sql = "SELECT * FROM {$this->table} 
                 WHERE ($whereIdSite enable_only_idsite = 0)
                   AND (enable_all_users = 1 OR login = ?)
                   AND deleted = 0
                   $extraWhere
              ORDER BY name ASC";
        
        return $this->db->fetchAll($sql, $bind);
    }

    public function install()
    {
        $query = 'CREATE TABLE `' . $this->table . '` (
                    `idsegment` INT(11) NOT NULL AUTO_INCREMENT,
                    `name` VARCHAR(255) NOT NULL,
                    `definition` TEXT NOT NULL,
                    `login` VARCHAR(100) NOT NULL,
                    `enable_all_users` tinyint(4) NOT NULL default 0,
                    `enable_only_idsite` INTEGER(11) NULL,
                    `auto_archive` tinyint(4) NOT NULL default 0,
                    `ts_created` TIMESTAMP NULL,
                    `ts_last_edit` TIMESTAMP NULL,
                    `deleted` tinyint(4) NOT NULL default 0,
                    PRIMARY KEY (`idsegment`)
                ) DEFAULT CHARSET=utf8';
        try {
            $this->db->query($query);
        } catch (\Exception $e) {
            if (!$this->db->isErrNo($e, '1050')) {
                throw $e;
            }
        }
    }

    public function uninstall()
    {
        $sql = 'DROP TABLE IF EXISTS ' . $this->table;
        $this->db->query($sql);
    }
}
