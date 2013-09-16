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

class User extends Base
{
    public function __construct($db, $table)
    {
        parent::__construct($db, $table);
//      $this->table = $db->quoteIdentifier($this->table);
    }

    public function getAll($userLogins = '')
    {
        $where = '';
        $bind = array();
        if (!empty($userLogins)) {
            $userLogins = explode(',', $userLogins);
            $where = 'WHERE login IN ('. Common::getSqlStringFieldsArray($userLogins).')';
            $bind = $userLogins;
        }
        $users = $this->db->fetchAll("SELECT * 
                                FROM ".$this->table."
                                $where 
                                ORDER BY login ASC", $bind);
        
        return $users;
    }

    public function getAllLogins()
    {
        $sql = 'SELECT login FROM ' . $this->table . ' ORDER BY login ASC';
        return $this->db->fetchAll($sql);
    }

    public function getUserByLogin($login)
    {
        return $this->db->fetchRow("SELECT * 
                                    FROM ".$this->table
                                  ." WHERE login = ?", $login);
    }

    public function getUserByEmail($email)
    {
        return $this->db->fetchRow("SELECT * 
                                    FROM ".$this->table
                                  ." WHERE email = ?", $email);
    }

    public function add($login, $pass, $alias, $email, $token, $date)
    {
        $this->db->insert($this->table, 
            array(
              'login' => $login,
              'password' => $pass,
              'alias' => $alias,
              'email' => $email,
              'token_auth' => $token,
              'date_registered' => $date
            )
        );
    }

    public function update($password, $alias, $email, $token_auth, $login)
    {
        $this->db->update($this->table,
            array(
                'password' => $password,
                'alias'    => $alias,
                'email'    => $email,
                'token_auth' => $token_auth
            ),
            "login = '$login'"
        );
    }

    public function deleteByLogin($login)
    {
        $this->db->query('DELETE FROM ' . $this->table . ' WHERE login = ?', $login);
    }

    public function getCountByLogin($login)
    {
        return $this->db->fetchOne('SELECT count(*) FROM ' . $this->table . ' WHERE login = ?', $login);
    }

    public function getCountByEmail($email)
    {
        return $this->db->fetchOne('SELECT count(*) FROM ' . $this->table . ' WHERE email = ?', $email);
    }

    public function getLoginByTokenAuth($tokenAuth)
    {
        $sql = 'SELECT login FROM ' . $this->table . ' WHERE token_auth = ?';
        return $this->db->fetchOne($sql, array($tokenAuth));
    }

    public function getTokenAuthByLogin($login)
    {
        $sql = 'SELECT token_auth FROM ' . $this->table . ' WHERE login = ?';
        return $this->db->fetchOne($sql, array($login));
    }
} 
