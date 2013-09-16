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
namespace Piwik\Db\DAO\Pgsql;

use Piwik\Db\Factory;

/**
 *  @package Piwik
 *  @subpackage Piwik_Db
 */

class UserLanguage extends \Piwik\Db\DAO\Mysql\UserLanguage
{
    public function __construct($db, $table)
    {
        parent::__construct($db, $table);
    }

    public function setForUser($login, $languageCode)
    {
        $generic = Factory::getGeneric($this->db);
        $generic->beginTransaction();
        $sql = 'SELECT login, language FROM ' . $this->table . ' '
             . 'WHERE login = ?';
        $row = $this->db->fetchRow($sql, array($login));
        if ($row) {
            $sql = 'UPDATE ' . $this->table . ' SET '
                 . ' language = ? '
                 . 'WHERE login = ?';
            $this->db->query($sql, array($languageCode, $login));
        }
        else {
            $this->db->insert(
                $this->table,
                array('login' => $login, 'language' => $languageCode)
            );
        }
        $generic->commit();
    }

    public function install()
    {
        $sql = "CREATE TABLE IF NOT EXISTS ". $this->table ." (
                  login VARCHAR(100) NOT NULL,
                  language VARCHAR(10) NOT NULL,
                  PRIMARY KEY (login)
                )" ;
        $this->db->query($sql);
    }
}
