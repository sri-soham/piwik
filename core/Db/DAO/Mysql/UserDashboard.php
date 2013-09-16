<?php
/**
 *  Piwik - Open source web analytics
 *
 *  @link http://piwik.org
 *  @license http://www.gnu.org/licenses/gpl-3.0.html. GPL v3 or later
 *
 *  @category Piwik
 *  @package Piwik
 */
namespace Piwik\Db\DAO\Mysql;

use Piwik\Db\DAO\Base;

/**
 *  @package Piwik
 *  @subpackage Piwik_Db
 */

class UserDashboard extends Base
{
    public function __construct($db, $table)
    {
        parent::__construct($db, $table);
    }

    /**
     * Records the layout in the DB for the given user.
     *
     * @param string $login
     * @param int $idDashboard
     * @param string $layout
     */
    public function saveLayout($login, $idDashboard, $layout)
    {
        $bind = array($login, $idDashboard, $layout, $layout);
        $sql = 'INSERT INTO ' . $this->table . ' (login, iddashboard, layout) '
             . 'VALUES (?, ?, ?) '
             . 'ON DUPLICATE KEY UPDATE '
             . '  layout = ? ';
        $this->db->query($sql, $bind);
    }

    /**
     * Updates the name of a dashboard
     *
     * @param string $login
     * @param int $idDashboard
     * @param string $name
     */
    public function updateName($login, $idDashboard, $name)
    {
        $bind = array($name, $login, $idDashboard);
        $sql = 'UPDATE ' . $this->table . ' SET '
             . ' name = ? '
             . 'WHERE login = ? AND iddashboard = ?';
        $this->db->query($sql, $bind);
    }

    public function getLayoutByLoginDashboard($login, $idDashboard)
    {
        $sql = 'SELECT layout FROM ' . $this->table . ' '
             . 'WHERE login = ? AND iddashboard = ?';
        $rows = $this->db->fetchAll($sql, array($login, $idDashboard));
        
        return $rows;
    }

    public function deleteByLoginDashboard($login, $idDashboard)
    {
        $sql = 'DELETE FROM ' . $this->table . ' '
             . 'WHERE login = ? AND iddashboard = ?';
        $this->db->query($sql, array($login, $idDashboard));
    }

    public function getNextIdByLogin($login)
    {
        $sql = 'SELECT MAX(iddashboard) + 1 FROM ' . $this->table . ' '
             . 'WHERE login = ?';
        $nextId = $this->db->fetchOne($sql, array($login));
        if (empty($nextId)) {
            $nextId = 1;
        }

        return $nextId;
    }

    public function newDashboard($login, $idDashboard, $name, $layout)
    {
        $sql = 'INSERT INTO ' . $this->table . ' '
             . '(login, iddashboard, name, layout) VALUES (?, ?, ?, ?)';
        $values = array($login, $idDashboard, $name, $layout);
        $this->db->query($sql, $values);
    }

    public function getByLogin($login)
    {
        $sql = 'SELECT iddashboard, name, layout FROM ' . $this->table . ' '
             . 'WHERE login = ? ORDER BY iddashboard';
        return $this->db->fetchAll($sql, array($login));
    }

    public function deleteByLogin($login)
    {
        $sql = 'DELETE FROM ' . $this->table . ' WHERE login = ?';
        $this->db->query($sql, array($login));
    }

    public function install()
    {
        // we catch the exception
        try {
            $sql = "CREATE TABLE ". $this->table ." (
                    login VARCHAR( 100 ) NOT NULL ,
                    iddashboard INT NOT NULL ,
                    name VARCHAR( 100 ) NULL DEFAULT NULL ,
                    layout TEXT NOT NULL,
                    PRIMARY KEY ( login , iddashboard )
                    )  DEFAULT CHARSET=utf8 " ;
            $this->db->exec($sql);
        } catch(Exception $e) {
            // mysql code error 1050:table already exists
            // see bug #153 http://dev.piwik.org/trac/ticket/153
            if (!$this->db->isErrNo($e, '1050')) {
                throw $e;
            }
        }
    }

    public function uninstall()
    {
        $sql = 'DROP TABLE ' . $this->table;
        $this->db->exec($sql);
    }
}
