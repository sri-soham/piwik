<?php
/**
 * Piwik - free/libre analytics platform
 *
 * @link http://piwik.org
 * @license http://www.gnu.org/licenses/gpl-3.0.html GPL v3 or later
 *
 */

namespace Piwik\Measurable\Settings;

use Piwik\Db;
use Piwik\Db\Factory;
use Piwik\Common;
use Piwik\Settings\Setting;

/**
 * Storage for site settings
 */
class Storage extends \Piwik\Settings\Storage
{
    private $idSite = null;

    /**
     * @var Db
     */
    private $db = null;

    private $toBeDeleted = array();

    public function __construct(Db\AdapterInterface $db, $idSite)
    {
        $this->db     = $db;
        $this->idSite = $idSite;
    }

    protected function deleteSettingsFromStorage()
    {
        $SiteSetting = Factory::getDAO('site_setting');
        $SiteSetting->deleteByIdsite($this->idSite);
    }

    public function deleteValue(Setting $setting)
    {
        $this->toBeDeleted[$setting->getName()] = true;
        parent::deleteValue($setting);
    }

    public function setValue(Setting $setting, $value)
    {
        $this->toBeDeleted[$setting->getName()] = false; // prevent from deleting this setting, we will create/update it
        parent::setValue($setting, $value);
    }

    /**
     * Saves (persists) the current setting values in the database.
     */
    public function save()
    {
        $SiteSetting = Factory::getDAO('site_setting');

        foreach ($this->toBeDeleted as $name => $delete) {
            if ($delete) {
                $SiteSetting->deleteByIdsiteAndSettingName($this->idSite, $name);
            }
        }

        $this->toBeDeleted = array();

        foreach ($this->settingsValues as $name => $value) {
            $value = serialize($value);
            $SiteSetting->upsert($this->idSite, $name, $value);
        }
    }

    protected function loadSettings()
    {
        $SiteSetting = Factory::getDAO('site_setting');
        $settings = $SiteSetting->getByIdsite($this->idSite);

        $flat = array();
        foreach ($settings as $setting) {
            $flat[$setting['setting_name']] = unserialize($setting['setting_value']);
        }

        return $flat;
    }
}
