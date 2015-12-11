<?php
/**
 * Piwik - free/libre analytics platform
 *
 * @link http://piwik.org
 * @license http://www.gnu.org/licenses/gpl-3.0.html GPL v3 or later
 *
 *
 */
namespace Piwik\Plugins\LanguagesManager;

use Piwik\Common;
use Piwik\Db;
use Piwik\DbHelper;

class Model implements \Piwik\Db\FactoryCreated
{
    protected static $rawPrefix = 'user_language';
    protected $table;
    protected $db;

    public function __construct()
    {
        $this->table = Common::prefixTable(self::$rawPrefix);
        $this->db = Db::get();
    }

    public function deleteUserLanguage($userLogin)
    {
        $this->db->query('DELETE FROM ' . $this->table . ' WHERE login = ?', $userLogin);
    }

    /**
     * Returns the language for the user
     *
     * @param string $userLogin
     * @return string
     */
    public function getLanguageForUser($userLogin)
    {
        return $this->db->fetchOne('SELECT language FROM ' . $this->table .
                            ' WHERE login = ? ', array($userLogin));
    }

    /**
     * Sets the language for the user
     *
     * @param string $login
     * @param string $languageCode
     * @return bool
     */
    public function setLanguageForUser($login, $languageCode)
    {
        $query = 'INSERT INTO ' . $this->table .
                 ' (login, language) VALUES (?,?) ON DUPLICATE KEY UPDATE language=?';
        $bind  = array($login, $languageCode, $languageCode);
        $this->db->query($query, $bind);

        return true;
    }

    public static function install()
    {
        $userLanguage = "login VARCHAR( 100 ) NOT NULL ,
					     language VARCHAR( 10 ) NOT NULL ,
					     PRIMARY KEY ( login )";
        DbHelper::createTable(self::$rawPrefix, $userLanguage);
    }

    public static function uninstall()
    {
        Db::dropTables(Common::prefixTable(self::$rawPrefix));
    }
}
