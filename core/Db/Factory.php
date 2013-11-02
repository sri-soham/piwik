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
 namespace Piwik\Db;

 use Piwik\Config;
 use Piwik\Db;

/**
 * @package Piwik
 * @subpackage Piwik_Db
 */
class Factory
{
    private static $daos  = array();
    private static $is_test = false;
    private static $instance = null;

    private $adapter;
    private $folder;

    private static function setInstance()
    {
        if (is_null(self::$instance)) {
            self::$instance = new self();
        }
    }

    # Used during installation. PDO_MYSQL is the default adapter. This is a
    # singleton class. PDO_MYSQL is loaded as default adapter for loading
    # the user language settings. After db settings are taken from user input
    # and stored in session, the "adapter" and the "folder" values have to
    # be changed.
    public static function refreshInstance()
    {
        self::$instance = new self();
        self::$daos = array();
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
            $db = Db::get();
        }

        $class_name = $this->getClassNameFromTableName($table);
        $class = new $class_name($db, $table);

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
            $db = Db::get();
        }

        $class_name = 'Piwik\\Db\\Helper\\' . $this->folder . '\\' . $class_name;
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
        $name = 'Piwik\\Db\\DAO\\' . $this->folder . '\\Generic';
        if ($db == null) {
            $db = Db::get();
        }
        $class = new $name($db);

        return $class;
    }

    /**
     *  get class name from table name
     *
     *  Returns the name of the dao class based on the table name and
     *  
     *  @param  String $table
     *  @return string
     */
    private function getClassNameFromTableName($table)
    {
        $class = $this->classFromTable($table);
        $fullClass = 'Piwik\\Db\\DAO\\' . $this->folder . '\\' . $class;
        $path = $this->fullPathFromClassName($fullClass);
        if (!file_exists($path)) {
            // dao classes of mysql are the base classes. If a dao class does
            // not exist, use the mysql dao class.
            $fullClass = 'Piwik\\Db\\DAO\\Mysql\\' . $class;
        }
        return $fullClass;
    }

    /**
     *  Returns the class name from the table name
     */
    private function classFromTable($table)
    {
        $parts = explode('_', $table);

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
            case 'pdo\pgsql':
                $ret = 'Pgsql';
            break;
            case 'pdo\mysql':
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
        $config = Config::getInstance();
        $database = $config->database;
        return $database['adapter'];
    }

    private function fullPathFromClassName($fullClassName)
    {
        $parts = explode('\\', $fullClassName);
        unset($parts[0]);
        return PIWIK_INCLUDE_PATH . '/core/' . implode(DIRECTORY_SEPARATOR, $parts) . '.php';
    }
}
