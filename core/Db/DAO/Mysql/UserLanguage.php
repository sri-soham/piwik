<?php
/**
 *  Piwik - Open source web analytics
 *
 *  @link http://piwik.org
 *  @license http://www.gnu.org/licenses/gpl-3.0.html GPL v3 or later
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

class UserLanguage extends Base
{
    public function __construct($db, $table)
    {
        parent::__construct($db, $table);
    }

    public function getByLogin($login)
    {
        $sql = 'SELECT language FROM '. $this->table . ' WHERE login = ?';
        return $this->db->fetchOne($sql, array($login));
    }

    public function setForUser($login, $languageCode)
    {
        $sql = 'INSERT INTO ' . $this->table . ' (login, language) VALUES (?, ?) '
             . 'ON DUPLICATE KEY UPDATE language = ?';
        $this->db->query($sql, array($login, $languageCode, $languageCode));
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
                    language VARCHAR( 10 ) NOT NULL ,
                    PRIMARY KEY ( login )
                    )  DEFAULT CHARSET=utf8 " ;
            $this->db->query($sql);
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
        $this->db->query($sql);
    }
}
