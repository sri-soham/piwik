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

use Piwik\Common;
use Piwik\Db\Factory;

/**
 * Option provides a very simple mechanism to save/retrieve key-values pair
 * from the database (persistent key-value datastore).
 *
 * This is useful to save Piwik-wide preferences, configuration values.
 *
 * @package Piwik
 */
class Option
{
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
    static public function getInstance()
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

    /**
     * Returns the option value for the requested option $name, fetching from database, if not in cache.
     *
     * @param string $name  Key
     * @return string|bool  Value or false, if not found
     */
    public function get($name)
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

    /**
     * Sets the option value in the database and cache
     *
     * @param string $name
     * @param string $value
     * @param int $autoLoad  if set to 1, this option value will be automatically loaded; should be set to 1 for options that will always be used in the Piwik request.
     */
    public function set($name, $value, $autoLoad = 0)
    {
        $autoLoad = (int)$autoLoad;
        $dao = Factory::getDAO('option');
        $dao->addRecord($name, $value, $autoLoad);
        $this->all[$name] = $value;
    }

    /**
     * Delete key-value pair from database and reload cache.
     *
     * @param string $name   Key to match exactly
     * @param string $value  Optional value
     */
    public function delete($name, $value = null)
    {
        $dao = Factory::getDAO('option');
        $dao->delete($name, $value);

        $this->clearCache();
    }

    /**
     * Delete key-value pair(s) from database and reload cache.
     * The supplied pattern should use '%' as wildcards, and literal '_' should be escaped.
     *
     * @param string $name   Pattern of key to match.
     * @param string $value  Optional value
     */
    public function deleteLike($name, $value = null)
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
    private function autoload()
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

    /**
     * Clears the cache
     * Used in unit tests to reset the state of the object between tests
     *
     * @return void
     */
    public function clearCache()
    {
        $this->loaded = false;
        $this->all = array();
    }
}
