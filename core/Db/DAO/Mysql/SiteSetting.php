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

use Piwik\Db\DAO\Base;

/**
 * @package Piwik
 * @subpackage Piwik_Db
 */

class SiteSetting extends Base
{ 
    public function __construct($db, $table)
    {
        parent::__construct($db, $table);
    }

    public function deleteByIdsite($idsite)
    {
        $sql   = "DELETE FROM {$this->table} WHERE idsite = ?";
        $bind  = array($idsite);
        $this->db->query($sql, $bind);
    }

    public function deleteByIdsiteAndSettingName($idSite, $settingName)
    {
        $sql  = "DELETE FROM {$this->table} WHERE idsite = ? and setting_name = ?";
        $bind = array($idSite, $settingName);
        $this->db->query($sql, $bind);
    }

    public function upsert($idSite, $settingName, $settingValue)
    {
        $sql  = "INSERT INTO $this->table (idsite, setting_name, setting_value) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE setting_value = ?";
        $bind = array($idSite, $settingName, $settingValue, $settingValue);
        $this->db->query($sql, $bind);
    }

    public function getByIdsite($idSite)
    {
        $sql  = "SELECT setting_name, setting_value FROM " . $this->table . " WHERE idsite = ?";
        $bind = array($idSite);

        return $this->db->fetchAll($sql, $bind);
    }
}

