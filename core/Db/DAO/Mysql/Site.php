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

/**
 * @package Piwik
 * @subpackage Piwik_Db
 */

class Site extends Base
{ 
    public function __construct($db, $table)
    {
        parent::__construct($db, $table);
    }

    public function getAllByGroup($group)
    {
        $sql = 'SELECT * FROM ' . $this->table . ' '
             . 'WHERE ' . $this->db->quoteIdentifier('group') . ' = ?';
        return $this->db->fetchAll($sql, $group);
    } 

    public function getAllGroups()
    {
        $group = $this->db->quoteIdentifier('group');
        $sql = "SELECT DISTINCT $group FROM {$this->table} ORDER BY $group";
        return $this->db->fetchAll($sql);
    }

    public function getByIdsite($idsite)
    {
        $sql = 'SELECT * FROM ' . $this->table . ' WHERE idsite = ?';
        return $this->db->fetchRow($sql, $idsite);
    }

    public function getAllIdsites()
    {
        $sql = 'SELECT idsite FROM ' . $this->table;
        return $this->db->fetchAll($sql);
    }

    public function getAll()
    { 
        $sql = 'SELECT * FROM ' . $this->table . ' ORDER BY idsite ASC';
        return $this->db->fetchAll($sql);
    }

    public function getIdsiteWithVisits($time, $now)
    {
        $sql = 'SELECT idsite '
             . 'FROM ' . $this->table . ' AS s '
             . 'WHERE EXISTS ( '
             . '    SELECT 1 '
             . '    FROM ' . Common::prefixTable('log_visit') . ' AS v '
             . '    WHERE v.idsite = s.idsite '
             . '      AND visit_last_action_time > ? '
             . '      AND visit_last_action_time <= ? '
             . '    LIMIT 1 '
             . ')';
        
        return $this->db->fetchAll($sql, array($time, $now));
    }

    public function getByIdsites($idsites, $limit = '')
    {
        if ($limit) {
            $limit = ' LIMIT ' . (int)$limit;
        }
        $sql = 'SELECT * FROM ' . $this->table . ' '
             . 'WHERE idsite IN (' . implode(', ', $idsites) . ') '
             . 'ORDER BY idsite ASC '
             . $limit;
        return $this->db->fetchAll($sql);
    }

    public function getIdsitesByUrlForSuperUser($url, $urlBis)
    {
        $sql = 'SELECT idsite '
             . 'FROM ' . $this->table . ' '
             . 'WHERE (main_url = ? OR main_url = ?) '
             . 'UNION '
             . 'SELECT idsite '
             . 'FROM ' . Common::prefixTable('site_url') . ' '
             . 'WHERE (url = ? OR url = ?) ';

        return $this->db->fetchAll($sql, array($url, $urlBis, $url, $urlBis));
    }

    public function getIdsiteByUrlForUser($url, $urlBis, $login)
    {
        $Access = Factory::getDAO('access');
        $sql = 'SELECT idsite '
             . 'FROM ' . $this->table . ' '
             . 'WHERE (main_url = ? OR main_url = ?) '
             . 'AND idsite IN ( ' . $Access->sqlAccessSiteByLogin(' idsite ') . ' ) '
             . 'UNION '
             . 'SELECT idsite '
             . 'FROM ' . Common::prefixTable('site_url') . ' '
             . 'WHERE (url = ? OR url = ?) '
             . '  AND idsite IN ( ' . $Access->sqlAccessSiteByLogin(' idsite ') . ' ) ';

        return $this->db->fetchAll($sql, array($url, $urlBis, $login, $url, $urlBis, $login));
    }

    public function getIdsitesByTimezones($timezones)
    {
        $sql = 'SELECT idsite FROM ' . $this->table . ' '
             . 'WHERE timezone in ( ' . Common::getSqlStringFieldsArray($timezones) . ' ) '
             . 'ORDER BY idsite ASC';

        return $this->db->fetchAll($sql, $timezones);
    }

    public function addRecord($bind)
    {
        $this->db->insert($this->table, $bind);
        return $this->db->lastInsertId();
    }

    public function deleteByIdsite($idsite)
    {
        $sql = 'DELETE FROM ' . $this->table . ' WHERE idsite = ?';
        $this->db->query($sql, $idsite);
    }

    public function updateByIdsite($values, $idsite)
    {
        $this->db->update($this->table, $values, "idsite = $idsite");
    }

    public function getDistinctTimezones()
    {
        $sql = 'SELECT distinct timezone FROM ' . $this->table;
        return $this->db->fetchAll($sql);
    }

    public function getSitesByPattern($pattern, $ids_str, $limit)
    {
        $bind = array();
        $bind[] = '%' . $pattern . '%';
        $bind[] = 'http%' . $pattern . '%';
        $where = '';
        if (is_numeric($pattern)) {
            $bind[] = $pattern;
            $where = ' OR idsite = ?';
        }
        $like = $this->likeKeyword();
        $sql = 'SELECT idsite, name, main_url '
             . 'FROM ' . $this->table . ' '
             . "WHERE (name $like ? OR main_url $like ? $where ) "
             . '  AND idsite in (' . $ids_str . ') '
             . 'LIMIT ' . $limit;

        return $this->db->fetchAll($sql, $bind);
    }

    public function addColFeedburnername()
    {
        $sql = 'ALTER TABLE ' . $this->table . ' ADD COLUMN feedburner_name VARCHAR(100) DEFAULT NULL';
        try {
            $this->db->exec($sql);
        } catch(\Exception $e) {
            // mysql code error 1060: column already exists
            // if there is another error we throw the exception, otherwise it is OK as we are simply reinstalling the plugin
            if(!$this->db->isErrNo($e, '1060')) {
                throw $e;
            }
        }
    }

    public function removeColFeedburnername()
    {
        $sql = 'ALTER TABLE ' . $this->table . ' DROP COLUMN feedburner_name';
        $this->db->exec($sql);
    }

    public function getFeedburnernameByIdsite($idsite)
    {
        $sql = 'SELECT feedburner_name FROM ' . $this->table . ' '
             . 'WHERE idsite = ?';
        $row = $this->db->fetchOne($sql, $idsite);
        if ($row) {
            $row['feedburnerName'] = $row['feedburner_name'];
            unset($row['feedburner_name']);
        }

        return $row;
    } 

    public function updateFeedburnername($feedburnername, $idsite)
    {
        $sql = 'UPDATE ' . $this->table . ' '
             . ' SET feedburner_name = ? '
             . 'WHERE idsite = ?';
        $this->db->query($sql, array($feedburnername, $idsite));
    }

    public function updateTSCreated($idsites, $date)
    {
        $sql = 'UPDATE ' . $this->table . ' '
             . ' SET ts_created = ? '
             . 'WHERE idsite IN (' . $idsites . ') '
             . '  AND ts_created > ?';
        $this->db->query($sql, array($date, $date));
    }

    public function updateTSCreatedByIdsite($ts_created, $idsite)
    {
        $this->db->update($this->table,
            array('ts_created' => $ts_created),
            "idsite = $idsite"
        );
    }

    // Mysql does case insensitive comparison for "LIKE" conditions
    // Pgsql does case sensitive comparison
    protected function likeKeyword()
    {
        return 'LIKE';
    }
}
