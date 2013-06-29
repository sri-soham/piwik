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

/**
 * @package Piwik
 * @subpackage Piwik_Db
 */
class Piwik_Db_Factory
{
    private static $daos  = array();
    private static $is_test = false;
    private static $instance = null;

    private $table;
    private $adapter;
    private $folder;
    private $base;
    private $derived;
    private $base_path;
    private $derived_path;

    private static function setInstance()
    {
        if (is_null(self::$instance)) {
            self::$instance = new self();
        }
    }

    public static function getDAO($table, $db=null)
    {
        self::setInstance();
        return self::$instance->dao($table, $db);
    }

    public static function getGeneric($db=null)
    {
        self::setInstance();
        return self::$instance->generic($db);
    }

    public static function getHelper($class_name, $db=null)
    {
        self::setInstance();
        return self::$instance->helper($class_name, $db);
    }

    public static function setTest($test)
    {
        if (is_bool($test)) {
            self::$is_test = $test;
        }
    }

    public function __construct()
    {
        $this->adapter = $this->getAdapter();
        $this->folder = $this->folderName();
    }

    /**
     *  dao
     *
     *  Returns the DAO class for the given table
     *
     *  @param string   $table
     *  @param object   $db
     *  @return mixed
     */
    public function dao($table, $db=null)
    {
        if (isset(self::$daos[$table]) && !self::$is_test) {
            return self::$daos[$table];
        }

        if (is_null($db)) {
            if(!empty($GLOBALS['PIWIK_TRACKER_MODE'])) {
                $db = Piwik_Tracker::getDatabase();
            }
            if($db === null) {
                $db = Zend_Registry::get('db');
            }
        }

        $this->setPropertiesByTableName($table);
        if (file_exists($this->derived_path)) {
            $class = new $this->derived($db, $table);
        }
        else {
            $class = new $this->base($db, $table);
        }

        self::$daos[$table] = $class;

        return $class;
    }

    /**
     *  helper
     *
     *  Returns the helper class with the given name. This is for classes that
     *  rely on database specific functionality but are not tied to any particular
     *  table. Eg. RankingQuery
     *
     *  @param string   $class_name
     *  @param object   $db
     *  @return mixed
     */
    public function helper($class_name, $db=null)
    {
        if (is_null($db)) {
            if(!empty($GLOBALS['PIWIK_TRACKER_MODE'])) {
                $db = Piwik_Tracker::getDatabase();
            }
            if($db === null) {
                $db = Zend_Registry::get('db');
            }
        }

        $class_name = 'Piwik_Db_Helper_' . $this->folder . '_' . $class_name;
        $class = new $class_name($db);

        return $class;
    }

    /**
     *  generic
     *
     *  Returns the class generic for the adapter for common actions.
     *  This is independent of any of the database tables.
     *
     *  @param resource $db
     *  @return mixed
     */
    public function generic($db)
    {
        $name = 'Piwik_Db_DAO_' . $this->folder . '_Generic';
        if ($db == null) {
            $db = Zend_Registry::get('db');
        }
        $class = new $name($db);

        return $class;
    }

    /**
     *  set properties by table name
     *
     *  Sets the properties required to get the dao and table name
     */
    private function setPropertiesByTableName($table)
    {
        $this->table = $table;
        $class = $this->classFromTable();
        $this->base = 'Piwik_Db_DAO_' . $class;
        $this->derived = 'Piwik_Db_DAO_' . $this->folder . '_' . $class;
        $this->base_path = $this->fullPathFromClassName($this->base);
        $this->derived_path = $this->fullPathFromClassName($this->derived);
    }

    /**
     *  Returns the class name from the table name
     */
    private function classFromTable()
    {
        $parts = explode('_', $this->table);

        foreach ($parts as $k=>$v) {
            $parts[$k] = ucfirst($v);
        }
        return implode('', $parts);
    }

    /**
     *  Returns the folder name based on the adapter
     */
    private function folderName()
    {
        $adapter = strtolower($this->adapter);
        switch ($adapter) {
            case 'pdo_pgsql':
                $ret = 'Pgsql';
            break;
            case 'pdo_mysql':
            case 'mysqli':
            default:
                $ret = 'Mysql';
            break;
        }

        return $ret;
    }

    /**
     *  Get adapter
     */
    private function getAdapter()
    {
        $config = Piwik_Config::getInstance();
        $database = $config->database;
        return $database['adapter'];
    }

    /**
     *  Full path from class name
     */
    private function fullPathFromClassName($class)
    {
        $parts = explode('_', $class);
        unset($parts[0]);

        return PIWIK_INCLUDE_PATH . '/core/' . implode(DIRECTORY_SEPARATOR, $parts) . '.php';
    }
}
