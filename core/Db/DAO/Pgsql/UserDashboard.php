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
namespace Piwik\Db\DAO\Pgsql;

use Piwik\Db\Factory;

/**
 *  @package Piwik
 *  @subpackage Piwik_Db
 */

class UserDashboard extends \Piwik\Db\DAO\Mysql\UserDashboard
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
        $generic = Factory::getGeneric($this->db);
        $sql = 'SELECT * FROM ' . $this->table . ' '
             . 'WHERE login = ? AND iddashboard = ? FOR UPDATE';
        $row = $this->db->query($sql, array($login, $idDashboard));
        if ($row) {
            $sql = 'UPDATE ' . $this->table . ' SET '
                 . ' layout = ? '
                 . 'WHERE login = ? AND iddashboard = ?';
            $bind = array($layout, $login, $idDashboard);
        }
        else {
            $sql = 'INSERT INTO ' . $this->table . '(login, iddashboard, layout) '
                 . 'VALUES (?, ?, ?)';
            $bind = array($login, $idDashboard, $layout);
        }
        $this->db->query($sql, $bind);
    }

    public function install()
    {
        // we catch the exception
        try {
            $sql = "CREATE TABLE IF NOT EXISTS ". $this->table ." (
                    login VARCHAR(100) NOT NULL ,
                    iddashboard INTEGER NOT NULL ,
                    name VARCHAR(100) NULL DEFAULT NULL ,
                    layout TEXT NOT NULL,
                    PRIMARY KEY (login , iddashboard)
                    )" ;
            $this->db->exec($sql);
        } catch (\Exception $e) {
            // postgresql code error 42P01: duplicate_table
            // see bug #153 http://dev.piwik.org/trac/ticket/153
            if ($e->getCode() != '42P01') {
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
