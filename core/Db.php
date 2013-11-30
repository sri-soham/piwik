<?php
/**
 * Piwik - Open source web analytics
 *
 * @link http://piwik.org
 * @license http://www.gnu.org/licenses/gpl-3.0.html GPL v3 or later
 *
 * @category Piwik
 * @package PluginsFunctions
 */
namespace Piwik;

use Exception;
use Piwik\Db\Adapter;
use Piwik\Db\Factory;
use Piwik\Tracker;

/**
 * Helper class that contains SQL related helper functions.
 * 
 * Plugins should use this class to execute SQL against the database.
 * 
 * ### Examples
 * 
 * **Basic Usage**
 * 
 *     $rows = Db::fetchAll("SELECT col1, col2 FROM mytable WHERE thing = ?", array('thingvalue'));
 *     foreach ($rows as $row) {
 *         doSomething($row['col1'], $row['col2']);
 *     }
 * 
 *     $value = Db::fetchOne("SELECT MAX(col1) FROM mytable");
 * 
 *     Db::query("DELETE FROM mytable WHERE id < ?", array(23));
 * 
 * @package PluginsFunctions
 * @api
 */
class Db
{
    private static $connection = null;

    /**
     * Returns the database connection and creates it if it hasn't been already.
     *
     * @return \Piwik\Tracker\Db|\Piwik\Db\AdapterInterface|\Piwik\Db
     */
    public static function get()
    {
        if (!empty($GLOBALS['PIWIK_TRACKER_MODE'])) {
            return Tracker::getDatabase();
        }

        if (self::$connection === null) {
            self::createDatabaseObject();
        }

        return self::$connection;
    }

    /**
     * Create the database object and connects to the database.
     * 
     * Shouldn't be called directly, use [get](#get).
     * 
     * @param array|null $dbInfos Connection parameters in an array. Defaults to the `[database]`
     *                            INI config section.
     */
    public static function createDatabaseObject($dbInfos = null)
    {
        $config = Config::getInstance();

        if (is_null($dbInfos)) {
            $dbInfos = $config->database;
        }

        /**
         * Triggered before a connection to the database is established.
         * 
         * This event can be used to dynamically change the settings used to connect to the
         * database.
         * 
         * @param array $dbInfos Reference to an array containing database connection info,
         *                       including:
         *                       - **host**: The host name or IP address to the MySQL database.
         *                       - **username**: The username to use when connecting to the
         *                                       database.
         *                       - **password**: The password to use when connecting to the
         *                                       database.
         *                       - **dbname**: The name of the Piwik MySQL database.
         *                       - **port**: The MySQL database port to use.
         *                       - **adapter**: either `'PDO_MYSQL'` or `'MYSQLI'`
         */
        Piwik::postEvent('Reporting.getDatabaseConfig', array(&$dbInfos));

        $dbInfos['profiler'] = $config->Debug['enable_sql_profiler'];

        $adapter = $dbInfos['adapter'];
        $db      = @Adapter::factory($adapter, $dbInfos);

        self::$connection = $db;
    }

    /**
     * Executes an unprepared SQL query. Recommended for DDL statements like CREATE,
     * DROP and ALTER. The return value is DBMS-specific. For MySQLI, it returns the
     * number of rows affected. For PDO, it returns the `Zend_Db_Statement` object.
     *
     * @param string $sql The SQL query.
     * @throws \Exception If there is an error in the SQL.
     * @return integer|\Zend_Db_Statement
     */
    static public function exec($sql)
    {
        /** @var \Zend_Db_Adapter_Abstract $db */
        $db = self::get();
        $profiler = $db->getProfiler();
        $q = $profiler->queryStart($sql, \Zend_Db_Profiler::INSERT);

        try {
            $return = self::get()->exec($sql);
        } catch (Exception $ex) {
            self::logExtraInfoIfDeadlock($ex);
            throw $ex;
        }

        $profiler->queryEnd($q);
        return $return;
    }

    /**
     * Executes an SQL query and returns the Zend_Db_Statement object.
     * If you want to fetch data from the DB you should use one of the fetch... functions.
     *
     * See also [http://framework.zend.com/manual/en/zend.db.statement.html](http://framework.zend.com/manual/en/zend.db.statement.html).
     *
     * @param string $sql The SQL query.
     * @param array $parameters Parameters to bind in the query, eg, `array(param1 => value1, param2 => value2)`.
     * @throws \Exception If there is a problem with the SQL or bind parameters.
     * @return \Zend_Db_Statement
     */
    static public function query($sql, $parameters = array())
    {
        try {
            return self::get()->query($sql, $parameters);
        } catch (Exception $ex) {
            self::logExtraInfoIfDeadlock($ex);
            throw $ex;
        }
    }

    /**
     * Executes the SQL query and fetches all the rows from the result set.
     *
     * @param string $sql The SQL query.
     * @param array $parameters Parameters to bind in the query, eg, `array(param1 => value1, param2 => value2)`.
     * @throws \Exception If there is a problem with the SQL or bind parameters.
     * @return array (one row in the array per row fetched in the DB)
     */
    static public function fetchAll($sql, $parameters = array())
    {
        try {
            return self::get()->fetchAll($sql, $parameters);
        } catch (Exception $ex) {
            self::logExtraInfoIfDeadlock($ex);
            throw $ex;
        }
    }

    /**
     * Executes an SQL query and fetches the first row of the result.
     *
     * @param string $sql The SQL query.
     * @param array $parameters Parameters to bind in the query, eg, `array(param1 => value1, param2 => value2)`.
     * @throws \Exception If there is a problem with the SQL or bind parameters.
     * @return array
     */
    static public function fetchRow($sql, $parameters = array())
    {
        try {
            return self::get()->fetchRow($sql, $parameters);
        } catch (Exception $ex) {
            self::logExtraInfoIfDeadlock($ex);
            throw $ex;
        }
    }

    /**
     * Executes an SQL query and fetches the first column of the first row of result set.
     *
     * @param string $sql The SQL query.
     * @param array $parameters Parameters to bind in the query, eg, `array(param1 => value1, param2 => value2)`.
     * @throws \Exception If there is a problem with the SQL or bind parameters.
     * @return string
     */
    static public function fetchOne($sql, $parameters = array())
    {
        try {
            return self::get()->fetchOne($sql, $parameters);
        } catch (Exception $ex) {
            self::logExtraInfoIfDeadlock($ex);
            throw $ex;
        }
    }

    /**
     * Executes an SQL query and returns the entire result set indexed by the first
     * selected field.
     *
     * @param string $sql The SQL query.
     * @param array $parameters Parameters to bind in the query, eg, `array(param1 => value1, param2 => value2)`.
     * @throws \Exception If there is a problem with the SQL or bind parameters.
     * @return array eg,
     *               ```
     *               array('col1value1' => array('col2' => '...', 'col3' => ...),
     *                     'col1value2' => array('col2' => '...', 'col3' => ...))
     *               ```
     */
    static public function fetchAssoc($sql, $parameters = array())
    {
        try {
            return self::get()->fetchAssoc($sql, $parameters);
        } catch (Exception $ex) {
            self::logExtraInfoIfDeadlock($ex);
            throw $ex;
        }
    }

    /**
     * Deletes all desired rows in a table, while using a limit. This function will execute many
     * DELETE queries until there are no more rows to delete.
     * 
     * Use this function when you need to delete many thousands of rows from a table without
     * locking the table for too long.
     * 
     * **Example**
     * 
     *     $idVisit = // ...
     *     Db::deleteAllRows(Common::prefixTable('log_visit'), "WHERE idvisit <= ?", "idvisit ASC", 100000, array($idVisit));
     * 
     * @param string $table The name of the table to delete from. Must be prefixed (see [Common::prefixTable](#)).
     * @param string $where The where clause of the query. Must include the WHERE keyword.
     * @param $orderBy The column to order by and the order by direction, eg, `idvisit ASC`.
     * @param int $maxRowsPerQuery The maximum number of rows to delete per DELETE query.
     * @param array $parameters Parameters to bind in the query.
     * @return int The total number of rows deleted.
     */
    static public function deleteAllRows($table, $where, $orderBy, $maxRowsPerQuery = 100000, $parameters = array())
    {
        $orderByClause = $orderBy ? "ORDER BY $orderBy" : "";
        $sql = "DELETE FROM $table
                $where
                $orderByClause
                LIMIT " . (int)$maxRowsPerQuery;

        // delete rows w/ a limit
        $totalRowsDeleted = 0;
        do {
            $rowsDeleted = self::query($sql, $parameters)->rowCount();

            $totalRowsDeleted += $rowsDeleted;
        } while ($rowsDeleted >= $maxRowsPerQuery);

        return $totalRowsDeleted;
    }

    /**
     * Runs an OPTIMIZE TABLE query on the supplied table or tables. The table names must be prefixed
     * (see [Common::prefixTable](#)).
     * 
     * Tables will only be optimized if the `[General] enable_sql_optimize_queries` config option is
     * set to **1**.
     *
     * @param string|array $tables The name of the table to optimize or an array of tables to optimize.
     * @return \Zend_Db_Statement
     */
    static public function optimizeTables($tables)
    {
        $optimize = Config::getInstance()->General['enable_sql_optimize_queries'];
        if (empty($optimize)) {
            return;
        }

        if (empty($tables)) {
            return false;
        }
        if (!is_array($tables)) {
            $tables = array($tables);
        }

        // filter out all InnoDB tables
        $nonInnoDbTables = array();
        foreach (Db::fetchAll("SHOW TABLE STATUS") as $row) {
            if (strtolower($row['Engine']) != 'innodb'
                && in_array($row['Name'], $tables)
            ) {
                $nonInnoDbTables[] = $row['Name'];
            }
        }

        if (empty($nonInnoDbTables)) {
            return false;
        }

        // optimize the tables
        return self::query("OPTIMIZE TABLE " . implode(',', $nonInnoDbTables));
    }

    /**
     * Drops the supplied table or tables. The table names must be prefixed (see [Common::prefixTable](#)).
     *
     * @param string|array $tables The name of the table to drop or an array of table names to drop.
     * @return \Zend_Db_Statement
     */
    static public function dropTables($tables)
    {
        if (!is_array($tables)) {
            $tables = array($tables);
        }

        return self::query("DROP TABLE " . implode(',', $tables));
    }

    /**
     * Locks the supplied table or tables. The table names must be prefixed (see [Common::prefixTable](#)).
     * 
     * **NOTE:** Piwik does not require the LOCK TABLES privilege to be available. Piwik
     * should still work in case it is not granted.
     * 
     * @param string|array $tablesToRead The table or tables to obtain 'read' locks on.
     * @param string|array $tablesToWrite The table or tables to obtain 'write' locks on.
     * @return \Zend_Db_Statement
     */
    static public function lockTables($tablesToRead, $tablesToWrite = array())
    {
        if (!is_array($tablesToRead)) {
            $tablesToRead = array($tablesToRead);
        }
        if (!is_array($tablesToWrite)) {
            $tablesToWrite = array($tablesToWrite);
        }

        $lockExprs = array();
        foreach ($tablesToWrite as $table) {
            $lockExprs[] = $table . " WRITE";
        }
        foreach ($tablesToRead as $table) {
            $lockExprs[] = $table . " READ";
        }

        return self::exec("LOCK TABLES " . implode(', ', $lockExprs));
    }

    /**
     * Releases all table locks.
     *
     * **NOTE:** Piwik does not require the LOCK TABLES privilege to be available. Piwik
     * should still work in case it is not granted.
     * 
     * @return \Zend_Db_Statement
     */
    static public function unlockAllTables()
    {
        return self::exec("UNLOCK TABLES");
    }

    /**
     * Performs a SELECT on a table one chunk at a time and returns the first
     * successfully fetched value.
     * 
     * In other words, if running a SELECT on one chunk of the table doesn't
     * return a value, we move on to the next chunk and we keep moving until
     * the SELECT returns a value.
     *
     * This function will break up a SELECT into several smaller SELECTs and
     * should be used when performing a SELECT that can take a long time to finish.
     * Using several smaller SELECTs will ensure that the table will not be locked
     * for too long.
     * 
     * **Example**
     * 
     *     // find the most recent visit that is older than a certain date 
     *     $dateStart = // ...
     *     $sql = "SELECT idvisit
     *           FROM $logVisit
     *          WHERE '$dateStart' > visit_last_action_time
     *            AND idvisit <= ?
     *            AND idvisit > ?
     *       ORDER BY idvisit DESC
     *          LIMIT 1";
     *
     *     // since visits
     *     return Db::segmentedFetchFirst($sql, $maxIdVisit, 0, -self::$selectSegmentSize);
     *
     * @param string $sql The SQL to perform. The last two conditions of the WHERE
     *                    expression must be as follows: 'id >= ? AND id < ?' where
     *                    'id' is the int id of the table.
     * @param int $first The minimum ID to loop from.
     * @param int $last The maximum ID to loop to.
     * @param int $step The maximum number of rows to scan in each smaller SELECT.
     * @param array $params Parameters to bind in the query, `array(param1 => value1, param2 => value2)`
     *
     * @return string
     */
    static public function segmentedFetchFirst($sql, $first, $last, $step, $params = array())
    {
        $result = false;
        if ($step > 0) {
            for ($i = $first; $result === false && $i <= $last; $i += $step) {
                $result = self::fetchOne($sql, array_merge($params, array($i, $i + $step)));
            }
        } else {
            for ($i = $first; $result === false && $i >= $last; $i += $step) {
                $result = self::fetchOne($sql, array_merge($params, array($i, $i + $step)));
            }
        }
        return $result;
    }

    /**
     * Performs a SELECT on a table one chunk at a time and returns an array
     * of every fetched value.
     * 
     * This function will break up a SELECT into several smaller SELECTs and
     * accumulate the result. It should be used when performing a SELECT that can
     * take a long time to finish. Using several smaller SELECTs will ensure that
     * the table will not be locked for too long.
     *
     * @param string $sql The SQL to perform. The last two conditions of the WHERE
     *                    expression must be as follows: 'id >= ? AND id < ?' where
     *                    'id' is the int id of the table.
     * @param int $first The minimum ID to loop from.
     * @param int $last The maximum ID to loop to.
     * @param int $step The maximum number of rows to scan in each smaller SELECT.
     * @param array $params Parameters to bind in the query, `array(param1 => value1, param2 => value2)`
     * @return array An array of primitive values.
     */
    static public function segmentedFetchOne($sql, $first, $last, $step, $params = array())
    {
        $result = array();
        if ($step > 0) {
            for ($i = $first; $i <= $last; $i += $step) {
                $result[] = self::fetchOne($sql, array_merge($params, array($i, $i + $step)));
            }
        } else {
            for ($i = $first; $i >= $last; $i += $step) {
                $result[] = self::fetchOne($sql, array_merge($params, array($i, $i + $step)));
            }
        }
        return $result;
    }

    /**
     * Performs a SELECT on a table one chunk at a time and returns an array
     * of every fetched row.
     *
     * This function will break up a SELECT into several smaller SELECTs and
     * accumulate the result. It should be used when performing a SELECT that can
     * take a long time to finish. Using several smaller SELECTs will ensure that
     * the table will not be locked for too long.
     * 
     * @param string $sql The SQL to perform. The last two conditions of the WHERE
     *                    expression must be as follows: 'id >= ? AND id < ?' where
     *                    'id' is the int id of the table.
     * @param int $first The minimum ID to loop from.
     * @param int $last The maximum ID to loop to.
     * @param int $step The maximum number of rows to scan in each smaller SELECT.
     * @param array $params Parameters to bind in the query, array( param1 => value1, param2 => value2)
     * @return array An array of rows that includes the result set of every executed
     *               query.
     */
    static public function segmentedFetchAll($sql, $first, $last, $step, $params = array())
    {
        $result = array();
        if ($step > 0) {
            for ($i = $first; $i <= $last; $i += $step) {
                $currentParams = array_merge($params, array($i, $i + $step));
                $result = array_merge($result, self::fetchAll($sql, $currentParams));
            }
        } else {
            for ($i = $first; $i >= $last; $i += $step) {
                $currentParams = array_merge($params, array($i, $i + $step));
                $result = array_merge($result, self::fetchAll($sql, $currentParams));
            }
        }
        return $result;
    }

    /**
     * Performs a non-SELECT query on a table one chunk at a time.
     * 
     * @param string $sql The SQL to perform. The last two conditions of the WHERE
     *                    expression must be as follows: 'id >= ? AND id < ?' where
     *                    'id' is the int id of the table.
     * @param int $first The minimum ID to loop from.
     * @param int $last The maximum ID to loop to.
     * @param int $step The maximum number of rows to scan in each smaller query.
     * @param array $params Parameters to bind in the query, `array(param1 => value1, param2 => value2)`
     */
    static public function segmentedQuery($sql, $first, $last, $step, $params = array())
    {
        if ($step > 0) {
            for ($i = $first; $i <= $last; $i += $step) {
                $currentParams = array_merge($params, array($i, $i + $step));
                self::query($sql, $currentParams);
            }
        } else {
            for ($i = $first; $i >= $last; $i += $step) {
                $currentParams = array_merge($params, array($i, $i + $step));
                self::query($sql, $currentParams);
            }
        }
    }

    /**
     * Attempts to get a named lock. This function uses a timeout of 1s, but will
     * retry a set number of time.
     *
     * @param string $lockName The lock name.
     * @param int $maxRetries The max number of times to retry.
     * @return bool `true` if the lock was obtained, `false` if otherwise.
     */
    static public function getDbLock($lockName, $maxRetries = 30)
    {
        /*
         * the server (e.g., shared hosting) may have a low wait timeout
         * so instead of a single GET_LOCK() with a 30 second timeout,
         * we use a 1 second timeout and loop, to avoid losing our MySQL
         * connection
         */
        $sql = 'SELECT GET_LOCK(?, 1)';

        $db = self::get();

        while ($maxRetries > 0) {
            if ($db->fetchOne($sql, array($lockName)) == '1') {
                return true;
            }
            $maxRetries--;
        }
        return false;
    }

    /**
     * Releases a named lock.
     *
     * @param string $lockName The lock name.
     * @return bool `true` if the lock was released, `false` if otherwise.
     */
    static public function releaseDbLock($lockName)
    {
        $sql = 'SELECT RELEASE_LOCK(?)';

        $db = self::get();
        return $db->fetchOne($sql, array($lockName)) == '1';
    }

    /**
     * Cached result of isLockprivilegeGranted function.
     *
     * Public so tests can simulate the situation where the lock tables privilege isn't granted.
     *
     * @var bool
     * @ignore
     */
    public static $lockPrivilegeGranted = null;

    /**
     * Checks whether the database user is allowed to lock tables.
     *
     * @return bool
     */
    public static function isLockPrivilegeGranted()
    {
        if (is_null(self::$lockPrivilegeGranted)) {
            $generic = Factory::getGeneric();
            self::$lockPrivilegeGranted = $generic->isLockPrivilegeGranted();
        }

        return self::$lockPrivilegeGranted;
    }

    private static function logExtraInfoIfDeadlock($ex)
    {
        if (self::get()->isErrNo($ex, 1213)) {
            $deadlockInfo = self::fetchAll("SHOW ENGINE INNODB STATUS");

            // log using exception so backtrace appears in log output
            Log::debug(new Exception("Encountered deadlock: " . print_r($deadlockInfo, true)));
        }
    }
}
