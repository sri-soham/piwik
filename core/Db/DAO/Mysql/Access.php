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

/**
 * @package Piwik
 * @subpackage Piwik_Db
 */

class Access extends Base
{
    public function __construct($db, $table)
    {
        parent::__construct($db, $table);
    }

    public function getAllByAccess($access)
    {
        $sql = "SELECT login, idsite FROM " . $this->table
             . " WHERE access = ? ORDER BY login, idsite";
        return $this->db->fetchAll($sql, $access);
    }

    public function getAllByIdsite($idsite)
    {
        $sql = "SELECT login, access FROM " . $this->table
             . " WHERE idsite = ?";
        return $this->db->fetchAll($sql, $idsite);
    }

    public function getAllByIdsiteAndAccess($idsite, $access)
    {
        $sql = "SELECT login FROM " . $this->table
             . " WHERE idsite = ? AND access = ?";
        return $this->db->fetchAll($sql, array($idsite, $access));
    }

    public function getAllByLogin($login)
    {
        $sql = "SELECT idsite, access FROM " . $this->table
             . " WHERE login = ?";
        return $this->db->fetchAll($sql, $login);
    }

    public function addIdSites($idsites, $login, $access)
    {
        foreach($idsites as $idsite) {
            $this->db->insert($this->table,
                array('idsite' => $idsite,
                      'login'  => $login,
                      'access' => $access
                )
            );
        }
    }

    public function deleteByLogin($login)
    {
        $sql = 'DELETE FROM ' . $this->table . ' WHERE login = ?';
        $this->db->query($sql, array($login));
    }

    public function deleteByIdsiteAndLogin($idsites, $login)
    {
        $sql = 'DELETE FROM ' . $this->table . ' WHERE idsite = ? AND login = ?';
        foreach($idsites as $idsite)
        {
            $this->db->query($sql, array($idsite, $login));
        }
    }

    public function getAccessSiteByLogin($login)
    {
        $sql = $this->sqlAccessSiteByLogin(' access, t2.idsite ');
        return $this->db->fetchAll($sql, array($login));
    }

    public function deleteByIdsite($idsite)
    {
        $sql = 'DELETE FROM ' . $this->table . ' WHERE idsite = ?';
        $this->db->query($sql, array($idsite));
    }

    /**
     * Returns the SQL query joining sites and access table for a given login
     * 
     * @param string $select Columns or expression to SELECT FROM table, eg. "MIN(ts_created)"
     * @return string SQL query
     */
    public function sqlAccessSiteByLogin($select)
    {
        $sql = 'SELECT ' . $select . ' FROM ' . $this->table . ' as t1 '
             . 'INNER JOIN ' . Common::prefixTable('site') . ' as t2 USING(idsite) '
             . 'WHERE login = ?';
        return $sql;
    }
}
