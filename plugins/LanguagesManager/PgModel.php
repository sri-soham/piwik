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

use Piwik\Db\Factory;

class PgModel extends Model
{
    /**
     * Sets the language for the user
     *
     * @param string $login
     * @param string $languageCode
     * @return bool
     */
    public function setLanguageForUser($login, $languageCode)
    {
        $Generic = Factory::getGeneric();
        $Generic->beginTransaction();
        $sql = 'SELECT * FROM ' . $this->table . ' WHERE login = ? FOR UPDATE';
        $row = $this->db->query($sql, array($login));
        if ($row) {
            $sql = 'UPDATE ' . $this->table . ' SET language = ? WHERE login = ?';
            $bind = array($languageCode, $login);
        }
        else {
            $sql = 'INSERT INTO ' . $this->table . ' (login, language) VALUES (?, ?)';
            $bind = array($login, $languageCode);
        }
        $this->db->query($sql, $bind);
        $Generic->commit();

        return true;
    }
}

