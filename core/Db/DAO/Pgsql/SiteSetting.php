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
namespace Piwik\Db\DAO\Pgsql;

use Piwik\Db\Factory;

/**
 * @package Piwik
 * @subpackage Piwik_Db
 */

class SiteSetting extends \Piwik\Db\DAO\Mysql\SiteSetting
{ 
    public function upsert($idSite, $settingName, $settingValue)
    {
        $Generic = Factory::getGeneric();
        $Generic->beginTransaction();
        $sql = "SELECT * FROM {$this->table} WHERE idsite = ? AND setting_name = ? FOR UPDATE";
        $row = $this->db->fetchOne($sql, array($idSite, $settingName));
        if ($row) {
            $sql = "UPDATE {$this->table} SET
                    setting_value = ?
                    WHERE idsite = ? AND setting_name = ?";
            $this->db->query($sql, array($settingValue, $idSite, $settingName));
        }
        else {
            $sql  = "INSERT INTO $this->table (idsite, setting_name, setting_value) VALUES (?, ?, ?)";
            $this->db->query($sql, array($idSite, $settingName, $settingValue));
        }
        $Generic->commit();
    }

}

