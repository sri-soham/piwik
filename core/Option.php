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
namespace Piwik;

use Piwik\Db\Factory;

/**
 * Convenient key-value storage for user specified options and temporary
 * data that needs to be persisted beyond one request.
 * 
 * ### Examples
 * 
 * **Setting and getting options**
 * 
 *     $optionValue = Option::get('MyPlugin.MyOptionName');
 *     if ($optionValue === false) {
 *         // if not set, set it
 *         Option::set('MyPlugin.MyOptionName', 'my option value');
 *     }
 * 
 * **Storing user specific options**
 * 
 *     $userName = // ...
 *     Option::set('MyPlugin.MyOptionName.' . $userName, 'my option value');
 * 
 * **Clearing user specific options**
 * 
 *     Option::deleteLike('MyPlugin.MyOptionName.%');
 *
 * @package Piwik
 * @api
 */
class Option
{
    /**
     * Returns the option value for the requested option `$name`.
     * 
     * @param string $name The option name.
     * @return string|false The value or `false`, if not found.
     */
    public static function get($name)
    {
        return self::getInstance()->getValue($name);
    }

    /**
     * Sets an option value by name.
     *
     * @param string $name The option name.
     * @param string $value The value to set the option to.
     * @param int $autoLoad If set to 1, this option value will be automatically loaded when Piwik is initialzed;
     *                      should be set to 1 for options that will be used in every Piwik request.
     */
    public static function set($name, $value, $autoload = 0)
    {
        return self::getInstance()->setValue($name, $value, $autoload);
    }

    /**
     * Deletes an option.
     *
     * @param string $name Option name to match exactly.
     * @param string $value If supplied the option will be deleted only if its value matches this value.
     */
    public static function delete($name, $value = null)
    {
        return self::getInstance()->deleteValue($name, $value);
    }

    /**
     * Deletes all options that match the supplied pattern.
     *
     * @param string $namePattern Pattern of key to match. `'%'` characters should be used as wildcards, and literal
     *                            `'_'` characters should be escaped.
     * @param string $value If supplied, options will be deleted only if their value matches this value.
     */
    public static function deleteLike($namePattern, $value = null)
    {
        return self::getInstance()->deleteNameLike($namePattern, $value);
    }

    /**
     * Clears the option value cache and forces a reload from the Database.
     * Used in unit tests to reset the state of the object between tests.
     *
     * @return void
     * @ignore
     */
    public static function clearCache()
    {
        $option = self::getInstance();
        $option->loaded = false;
        $option->all = array();
    }

    /**
     * @var array
     */
    private $all = array();

    /**
     * @var bool
     */
    private $loaded = false;

    /**
     * Singleton instance
     * @var \Piwik\Option
     */
    static private $instance = null;

    /**
     * Returns Singleton instance
     *
     * @return \Piwik\Option
     */
    static private function getInstance()
    {
        if (self::$instance == null) {
            self::$instance = new self;
        }
        return self::$instance;
    }

    /**
     * Private Constructor
     */
    private function __construct()
    {
    }

    protected function getValue($name)
    {
        $this->autoload();
        if(isset($this->all[$name])) {
            return $this->all[$name];
        }
        
        $dao = Factory::getDAO('option');
        $value = $dao->getValueByName($name);
        if($value === false) {
            return false;
        }
        $this->all[$name] = $value;
        return $value;
    }

    protected function setValue($name, $value, $autoLoad = 0)
    {
        $autoLoad = (int)$autoLoad;
        $dao = Factory::getDAO('option');
        $dao->addRecord($name, $value, $autoLoad);
        $this->all[$name] = $value;
    }

    protected function deleteValue($name, $value)
    {
        $dao = Factory::getDAO('option');
        $dao->delete($name, $value);

        $this->clearCache();
    }

    protected function deleteNameLike($name, $value = null)
    {
        $dao = Factory::getDAO('option');
        $dao->deleteLike($name, $value);

        $this->clearCache();
    }

    /**
     * Initialize cache with autoload settings.
     *
     * @return void
     */
    protected function autoload()
    {
        if ($this->loaded) {
            return;
        }

        $dao = Factory::getDAO('option');
        $all = $dao->getAllAutoload();
        foreach ($all as $option) {
            $this->all[$option['option_name']] = $option['option_value'];
        }

        $this->loaded = true;
    }
}
