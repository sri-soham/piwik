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
namespace Piwik\Db\Schema;

use Exception;
use Piwik\Common;
use Piwik\Config;
use Piwik\Date;
use Piwik\Db;
use Piwik\DbHelper;
use Piwik\Db\SchemaInterface;
use Piwik\Piwik;

/**
 * PgSql schema
 *
 * @package Piwik
 * @subpackage Piwik_Db
 */

class Pgsql implements SchemaInterface
{
    private $tablesInstalled = null;
    
    /**
     * Is this schema available?
     *
     * @return bool  True if schema is available; false otherwise
     */
    static public function isAvailable()
    {
        return true;
    }

    /**
     * Get the SQL to create Piwik tables
     *
     * @return array  array of strings containing SQL
     */
    public function getTablesCreateSql()
    {
        $config = Config::getInstance();
        $prefixTables = $config->database['tables_prefix'];
        $tables = array(
            'user' => 'CREATE TABLE "'.$prefixTables.'user"(
                          login VARCHAR(100) NOT NULL,
                          password CHAR(32) NOT NULL,
                          alias VARCHAR(45) NOT NULL,
                          email VARCHAR(100) NOT NULL,
                          token_auth VARCHAR(32) NOT NULL,
                          date_registered TIMESTAMP NULL,
                          PRIMARY KEY(login),
                          UNIQUE (token_auth)
                        )
            ',

            'access' => "CREATE TABLE {$prefixTables}access (
                          login VARCHAR(100) NOT NULL,
                          idsite BIGINT NOT NULL,
                          access VARCHAR(10) NULL,
                          PRIMARY KEY(login, idsite)
                        )
            ",

            'site' => 'CREATE TABLE "'.$prefixTables.'site" (
                          idsite SERIAL8 NOT NULL,
                          name VARCHAR(90) NOT NULL,
                          main_url VARCHAR(255) NOT NULL,
                          ts_created TIMESTAMP NULL,
                          ecommerce SMALLINT DEFAULT 0,
                          sitesearch SMALLINT DEFAULT 1,
                          sitesearch_keyword_parameters TEXT NOT NULL,
                          sitesearch_category_parameters TEXT NOT NULL,
                          timezone VARCHAR( 50 ) NOT NULL,
                          currency VARCHAR( 3 ) NOT NULL,
                          excluded_ips TEXT NOT NULL,
                          excluded_parameters  TEXT NOT NULL,
                          excluded_user_agents TEXT NOT NULL,
                          "group" VARCHAR(250) NOT NULL, 
                          "type" VARCHAR(255) NOT NULL,
                          keep_url_fragment SMALLINT DEFAULT 0,
                          PRIMARY KEY(idsite)
                        )
            ',

            'site_url' => "CREATE TABLE {$prefixTables}site_url (
                              idsite BIGINT NOT NULL,
                              url VARCHAR(255) NOT NULL,
                              PRIMARY KEY(idsite, url)
                        )
            ",

            'goal' => " CREATE TABLE {$prefixTables}goal (
                              idsite INT NOT NULL,
                              idgoal INT NOT NULL,
                              name VARCHAR(50) NOT NULL,
                              match_attribute VARCHAR(20) NOT NULL,
                              pattern VARCHAR(255) NOT NULL,
                              pattern_type VARCHAR(10) NOT NULL,
                              case_sensitive SMALLINT NOT NULL,
                              allow_multiple SMALLINT NOT NULL,
                              revenue FLOAT NOT NULL,
                              deleted SMALLINT NOT NULL default 0,
                              PRIMARY KEY  (idsite,idgoal)
                            )
            ",

            'logger_message'        => "CREATE TABLE {$prefixTables}logger_message (
                                      idlogger_message SERIAL8 NOT NULL,
                                      tag VARCHAR(50) NULL,
                                      timestamp TIMESTAMP NULL,
                                      level VARCHAR(16) NULL,
                                      message TEXT NULL,
                                      PRIMARY KEY(idlogger_message)
                                    )
            ",

            'log_action' => "CREATE TABLE {$prefixTables}log_action (
                                      idaction SERIAL8 NOT NULL,
                                      name TEXT,
                                      hash BIGINT NOT NULL,
                                      type SMALLINT NULL,
                                      url_prefix SMALLINT NULL,
                                      PRIMARY KEY(idaction)
                        )
            ",

            'log_visit' => "CREATE TABLE {$prefixTables}log_visit (
                              idvisit SERIAL8 NOT NULL,
                              idsite BIGINT NOT NULL,
                              idvisitor BYTEA NOT NULL,
                              visitor_localtime TIME NOT NULL,
                              visitor_returning  SMALLINT NOT NULL,
                              visitor_count_visits SMALLINT NOT NULL,
                              visitor_days_since_last SMALLINT NOT NULL,
                              visitor_days_since_order SMALLINT NOT NULL,
                              visitor_days_since_first SMALLINT NOT NULL,
                              visit_first_action_time TIMESTAMP NOT NULL,
                              visit_last_action_time TIMESTAMP NOT NULL,
                              visit_exit_idaction_url INTEGER DEFAULT 0,
                              visit_exit_idaction_name INTEGER NOT NULL,
                              visit_entry_idaction_url INTEGER NOT NULL,
                              visit_entry_idaction_name INTEGER NOT NULL,
                              visit_total_actions SMALLINT NOT NULL,
                              visit_total_searches SMALLINT NOT NULL,
                              visit_total_events SMALLINT NOT NULL,
                              visit_total_time SMALLINT NOT NULL,
                              visit_goal_converted SMALLINT NOT NULL,
                              visit_goal_buyer SMALLINT NOT NULL, 
                              referer_type SMALLINT NULL,
                              referer_name VARCHAR(70) NULL,
                              referer_url TEXT NOT NULL,
                              referer_keyword VARCHAR(255) NULL,
                              config_id BYTEA NOT NULL,
                              config_os CHAR(3) NOT NULL,
                              config_browser_name VARCHAR(10) NOT NULL,
                              config_browser_version VARCHAR(20) NOT NULL,
                              config_resolution VARCHAR(9) NOT NULL,
                              config_pdf SMALLINT NOT NULL,
                              config_flash SMALLINT NOT NULL,
                              config_java SMALLINT NOT NULL,
                              config_director SMALLINT NOT NULL,
                              config_quicktime SMALLINT NOT NULL,
                              config_realplayer SMALLINT NOT NULL,
                              config_windowsmedia SMALLINT NOT NULL,
                              config_gears SMALLINT NOT NULL,
                              config_silverlight SMALLINT NOT NULL,
                              config_cookie SMALLINT NOT NULL,
                              location_ip BYTEA NOT NULL,
                              location_browser_lang VARCHAR(20) NOT NULL,
                              location_country VARCHAR(3) NOT NULL,
                              location_region VARCHAR(2) DEFAULT NULL,
                              location_city VARCHAR(255) DEFAULT NULL,
                              location_latitude DOUBLE PRECISION DEFAULT NULL,
                              location_longitude DOUBLE PRECISION DEFAULT NULL,
                              custom_var_k1 VARCHAR(200) DEFAULT NULL,
                              custom_var_v1 VARCHAR(200) DEFAULT NULL,
                              custom_var_k2 VARCHAR(200) DEFAULT NULL,
                              custom_var_v2 VARCHAR(200) DEFAULT NULL,
                              custom_var_k3 VARCHAR(200) DEFAULT NULL,
                              custom_var_v3 VARCHAR(200) DEFAULT NULL,
                              custom_var_k4 VARCHAR(200) DEFAULT NULL,
                              custom_var_v4 VARCHAR(200) DEFAULT NULL,
                              custom_var_k5 VARCHAR(200) DEFAULT NULL,
                              custom_var_v5 VARCHAR(200) DEFAULT NULL,
                              PRIMARY KEY(idvisit)
                            )
            ",
        
            'log_conversion_item' => "CREATE TABLE {$prefixTables}log_conversion_item (
                                                  idsite BIGINT NOT NULL,
                                                  idvisitor BYTEA NOT NULL,
                                                  server_time TIMESTAMP NOT NULL,
                                                  idvisit BIGINT NOT NULL,
                                                  idorder VARCHAR(100) NOT NULL,
                                                  
                                                  idaction_sku BIGINT NOT NULL,
                                                  idaction_name BIGINT NOT NULL,
                                                  idaction_category BIGINT NOT NULL,
                                                  idaction_category2 BIGINT NOT NULL,
                                                  idaction_category3 BIGINT NOT NULL,
                                                  idaction_category4 BIGINT NOT NULL,
                                                  idaction_category5 BIGINT NOT NULL,
                                                  price FLOAT NOT NULL,
                                                  quantity BIGINT NOT NULL,
                                                  deleted BOOLEAN NOT NULL,
                                                  
                                                  PRIMARY KEY(idvisit, idorder, idaction_sku)
                                                )
            ",

            'log_conversion' => "CREATE TABLE {$prefixTables}log_conversion (
                                      idvisit BIGINT NOT NULL,
                                      idsite BIGINT NOT NULL,
                                      idvisitor BYTEA NOT NULL,
                                      server_time TIMESTAMP NOT NULL,
                                      idaction_url INTEGER DEFAULT NULL,
                                      idlink_va INTEGER DEFAULT NULL,
                                      referer_visit_server_date DATE DEFAULT NULL,
                                      referer_type INTEGER DEFAULT NULL,
                                      referer_name VARCHAR(70) DEFAULT NULL,
                                      referer_keyword VARCHAR(255) DEFAULT NULL,
                                      visitor_returning SMALLINT NOT NULL,
                                      visitor_count_visits SMALLINT NOT NULL,
                                      visitor_days_since_first SMALLINT NOT NULL,
                                      visitor_days_since_order SMALLINT NOT NULL,
                                      location_country VARCHAR(3) NOT NULL,
                                      location_region VARCHAR(2) DEFAULT NULL,
                                      location_city VARCHAR(255) DEFAULT NULL,
                                      location_latitude DOUBLE PRECISION DEFAULT NULL,
                                      location_longitude DOUBLE PRECISION DEFAULT NULL,
                                      url TEXT,
                                      idgoal INTEGER NOT NULL,
                                      buster BIGINT NOT NULL,
                                      idorder VARCHAR(100) DEFAULT NULL,
                                      items SMALLINT DEFAULT NULL,
                                      revenue FLOAT DEFAULT NULL,
                                      revenue_subtotal FLOAT DEFAULT NULL,
                                      revenue_tax FLOAT DEFAULT NULL,
                                      revenue_shipping FLOAT DEFAULT NULL,
                                      revenue_discount FLOAT DEFAULT NULL,
                                      custom_var_k1 VARCHAR(200) DEFAULT NULL,
                                      custom_var_v1 VARCHAR(200) DEFAULT NULL,
                                      custom_var_k2 VARCHAR(200) DEFAULT NULL,
                                      custom_var_v2 VARCHAR(200) DEFAULT NULL,
                                      custom_var_k3 VARCHAR(200) DEFAULT NULL,
                                      custom_var_v3 VARCHAR(200) DEFAULT NULL,
                                      custom_var_k4 VARCHAR(200) DEFAULT NULL,
                                      custom_var_v4 VARCHAR(200) DEFAULT NULL,
                                      custom_var_k5 VARCHAR(200) DEFAULT NULL,
                                      custom_var_v5 VARCHAR(200) DEFAULT NULL,
                                      PRIMARY KEY (idvisit, idgoal, buster),
                                      UNIQUE (idsite, idorder)
                                    )
            ",

            'log_link_visit_action' => "CREATE TABLE {$prefixTables}log_link_visit_action (
                                              idlink_va SERIAL NOT NULL,
                                              idsite BIGINT NOT NULL,
                                              idvisitor BYTEA NOT NULL,
                                              server_time TIMESTAMP NOT NULL,
                                              idvisit BIGINT NOT NULL,
                                              idaction_url INTEGER DEFAULT NULL,
                                              idaction_url_ref INTEGER DEFAULT 0,
                                              idaction_name INTEGER,
                                              idaction_name_ref INTEGER NOT NULL,
                                              idaction_event_category INTEGER,
                                              idaction_event_action INTEGER,
                                              time_spent_ref_action INTEGER NOT NULL,
                                              custom_var_k1 VARCHAR(200) DEFAULT NULL,
                                              custom_var_v1 VARCHAR(200) DEFAULT NULL,
                                              custom_var_k2 VARCHAR(200) DEFAULT NULL,
                                              custom_var_v2 VARCHAR(200) DEFAULT NULL,
                                              custom_var_k3 VARCHAR(200) DEFAULT NULL,
                                              custom_var_v3 VARCHAR(200) DEFAULT NULL,
                                              custom_var_k4 VARCHAR(200) DEFAULT NULL,
                                              custom_var_v4 VARCHAR(200) DEFAULT NULL,
                                              custom_var_k5 VARCHAR(200) DEFAULT NULL,
                                              custom_var_v5 VARCHAR(200) DEFAULT NULL,
                                              custom_float DOUBLE PRECISION DEFAULT NULL,
                                              PRIMARY KEY(idlink_va)
                                            )
            ",

            'log_profiling' => "CREATE TABLE {$prefixTables}log_profiling (
                                  query TEXT NOT NULL,
                                  count INTEGER NULL,
                                  sum_time_ms FLOAT NULL,
                                  UNIQUE (query)
                                )
            ",

            'option' => 'CREATE TABLE "'.$prefixTables.'option" (
                                option_name VARCHAR( 255 ) NOT NULL,
                                option_value TEXT NOT NULL,
                                autoload BOOLEAN DEFAULT \'1\',
                                PRIMARY KEY ( option_name )
                                )
            ',

            'session' => "CREATE TABLE {$prefixTables}session (
                                id CHAR(32) NOT NULL,
                                modified INTEGER,
                                lifetime INTEGER,
                                data TEXT,
                                PRIMARY KEY ( id )
                                )
            ",

            'archive_numeric'   => "CREATE TABLE {$prefixTables}archive_numeric (
                                      idarchive BIGINT NOT NULL,
                                      name VARCHAR(255) NOT NULL,
                                      idsite BIGINT NULL,
                                      date1 DATE NULL,
                                      date2 DATE NULL,
                                      period SMALLINT NULL,
                                      ts_archived TIMESTAMP NULL,
                                      value FLOAT NULL,
                                      PRIMARY KEY(idarchive, name)
                                    )
            ",

            'archive_blob'  => "CREATE TABLE {$prefixTables}archive_blob (
                                      idarchive BIGINT NOT NULL,
                                      name VARCHAR(255) NOT NULL,
                                      idsite BIGINT NULL,
                                      date1 DATE NULL,
                                      date2 DATE NULL,
                                      period SMALLINT NULL,
                                      ts_archived TIMESTAMP NULL,
                                      value BYTEA NULL,
                                      PRIMARY KEY(idarchive, name)
                                    )
            ",
        );
        return $tables;
    }

    /**
     *  Get the SQL to create the indexes
     *
     *  @return array of strings containing SQL
     */
    public function getIndexesCreateSql()
    {
        $config = Config::getInstance();
        $prefix = $config->database['tables_prefix'];

        $indexes = array(
            'log_action' => array(
                "CREATE INDEX la_ix_type_hash ON {$prefix}log_action (type, hash)",
            ),
            'log_visit' => array(
                "CREATE INDEX lv_ix_id_config_datetime ON {$prefix}log_visit (idsite, config_id, visit_last_action_time)",
                "CREATE INDEX lv_ix_id_datetime ON {$prefix}log_visit (idsite, visit_last_action_time)",
                "CREATE INDEX lv_ix_site_visitor ON {$prefix}log_visit (idsite, idvisitor)",
            ),
            'log_conversion_item' => array(
                "CREATE INDEX lci_ix_idsite_servertime ON {$prefix}log_conversion_item (idsite, server_time)",
            ),
            'log_conversion' => array(
                "CREATE INDEX lc_ix_idsite_datetime ON {$prefix}log_conversion (idsite, server_time)",
            ),
            'log_link_visit_action' => array(
                "CREATE INDEX llvc_ix_idvisit On {$prefix}log_link_visit_action (idvisit)",
                "CREATE INDEX llvc_ix_idsite_servertime ON {$prefix}log_link_visit_action (idsite, server_time)",
            ),
            'option' => array(
                'CREATE INDEX option_ix_autoload ON "'.$prefix.'option" (autoload)',
            ),
            // #table# will be replaced with the name like archive_numeric_2012_07 etc. 
            'archive_numeric' => array(
                "CREATE INDEX #table#_ix_idp ON #table#(idsite, date1, date2, period, ts_archived)",
                "CREATE INDEX #table#_ix_pa ON #table#(period, ts_archived)",
            ),
            'archive_blob' => array(
                "CREATE INDEX #table#_ix_pa ON #table#(period, ts_archived)"
            )
        );

        return $indexes;
    }

    /**
     * Get the SQL to create functions
     *
     * @return array
     */
    public function getFunctionsCreateSql()
    {
        $functions = array(
            'is_numeric' => "CREATE OR REPLACE FUNCTION is_numeric(str TEXT) RETURNS BOOLEAN AS $$
                                DECLARE
                                    v_res BOOLEAN := false;
                                BEGIN
                                    SELECT str ~ '^(-)?[0-9]+(.[0-9]+)?$' INTO v_res;
                                    RETURN v_res;
                                END;
                                $$ LANGUAGE plpgsql",
            'hour' => "CREATE OR REPLACE FUNCTION hour(p_value TIME) RETURNS INT AS $$
                        BEGIN
                            RETURN EXTRACT(HOUR FROM p_value);
                        END;
                        $$ LANGUAGE plpgsql",
            'hour2' => "CREATE OR REPLACE FUNCTION hour(p_value TIMESTAMP) RETURNS INT AS $$
                            BEGIN
                                RETURN EXTRACT(HOUR FROM p_value);
                            END;
                            $$ LANGUAGE plpgsql"
        );

        return $functions;
    }

    /**
     * Get the SQL to create a specific Piwik table
     *
     * @param string  $tableName
     * @throws Exception
     * @return string  SQL
     */
    public function getTableCreateSql( $tableName )
    {
        $tables = DbHelper::getTablesCreateSql();

        if(!isset($tables[$tableName]))
        {
            throw new Exception("The table '$tableName' SQL creation code couldn't be found.");
        }

        return $tables[$tableName];
    }

    /**
     * Names of all the prefixed tables in piwik
     * Doesn't use the DB
     *
     * @return array  Table names
     */
    public function getTablesNames()
    {
        $aTables = array_keys($this->getTablesCreateSql());
        $config = Config::getInstance();
        $prefixTables = $config->database['tables_prefix'];
        $return = array();
        foreach($aTables as $table)
        {
            $return[] = $prefixTables.$table;
        }
        return $return;
    }

    /**
     * Get list of tables installed
     *
     * @param bool  $forceReload  Invalidate cache
     * @return array  installed Tables
     */
    public function getTablesInstalled($forceReload = true)
    {
        if(is_null($this->tablesInstalled)
            || $forceReload === true)
        {
            $db = Db::get();
            $config = Config::getInstance();
            $prefixTables = $config->database['tables_prefix'];

            // '_' matches any character; force it to be literal
            $prefixTables = str_replace('_', '\_', $prefixTables);

            $allTables = $db->fetchCol("SELECT tablename FROM pg_tables WHERE tablename LIKE '".$prefixTables."%'");

            // all the tables to be installed
            $allMyTables = $this->getTablesNames();

            // we get the intersection between all the tables in the DB and the tables to be installed
            $tablesInstalled = array_intersect($allMyTables, $allTables);

            // at this point we have only the piwik tables which is good
            // but we still miss the piwik generated tables (using the class Piwik_TablePartitioning)
            $allArchiveNumeric = $db->fetchCol("SELECT tablename FROM pg_tables WHERE tablename LIKE '".$prefixTables."archive_numeric%'");
            $allArchiveBlob = $db->fetchCol("SELECT tablename FROM pg_tables WHERE tablename LIKE '".$prefixTables."archive_blob%'");

            $allTablesReallyInstalled = array_merge($tablesInstalled, $allArchiveNumeric, $allArchiveBlob);

            $this->tablesInstalled = $allTablesReallyInstalled;
        }
        return  $this->tablesInstalled;
    }

    /**
     * Checks whether any table exists
     *
     * @return bool  True if tables exist; false otherwise
     */
    public function hasTables()
    {
        return count($this->getTablesInstalled()) != 0;
    }

    /**
     * Create database
     *
     * @param string  $dbName  Name of the database to create
     */
    public function createDatabase( $dbName = null )
    {
        if(is_null($dbName))
        {
            $dbName = Config::getInstance()->database['dbname'];
        }
        $sql = 'SELECT datname FROM pg_database where datname = ?';
        $one = Db::fetchOne($sql, array($dbName));
        if (empty($one))
        {
            Db::exec("CREATE DATABASE ".$dbName." ENCODING 'UTF8'");
            Db::exec("ALTER DATABASE $dbName SET bytea_output TO 'escape'");
        }
    }

    /**
     * Drop database
     *
     * Postgresql doesn't allow 'drop database ' command on the database to
     * which we are connected. So, dropping tables instead of dropping the
     * database and recreating it.
     */
    public function dropDatabase()
    {
        $this->dropTables();

        $config = Config::getInstance();
        $prefix = $config->database['tables_prefix'];
        $db = Db::get();
        $tables = array('report', 'segment');
        foreach ($tables as $table) {
            $db->query('DROP TABLE IF EXISTS "'.$prefix.$table.'"');
        }
    }

    /**
     * Create all tables
     */
    public function createTables()
    {
        $db = Db::get();
        $config = Config::getInstance();
        $prefixTables = $config->database['tables_prefix'];

        $tablesAlreadyInstalled = $this->getTablesInstalled();
        $tablesToCreate = $this->getTablesCreateSql();
        unset($tablesToCreate['archive_blob']);
        unset($tablesToCreate['archive_numeric']);

        foreach($tablesToCreate as $tableName => $tableSql)
        {
            $tableName = $prefixTables . $tableName;
            if(!in_array($tableName, $tablesAlreadyInstalled))
            {
                $db->query( $tableSql );
            }
        }

        $indexes = $this->getIndexesCreateSql();
        foreach ($indexes as $table => $sqls) {
            if ($table == 'archive_numeric' || $table == 'archive_blob') {
                continue;
            }
            
            foreach ($sqls as $sql) {
                $db->query($sql);
            }
        }

        $functions = $this->getFunctionsCreateSql();
        foreach ($functions as $name => $sql) {
            $db->query($sql);
        }
    }

    /**
     * Creates an entry in the User table for the "anonymous" user.
     */
    public function createAnonymousUser()
    {
        // The anonymous user is the user that is assigned by default
        // note that the token_auth value is anonymous, which is assigned by default as well in the Login plugin
        $db = Db::get();
        $sql = 'INSERT INTO "' . Common::prefixTable('user'). '" '
             . "VALUES ('anonymous', '', 'anonymous', 'anonymous@example.org', 'anonymous', '".Date::factory('now')->getDateTime()."' );";
        $db->query($sql);
    }

    /**
     * Truncate all tables
     */
    public function truncateAllTables()
    {
        $tablesAlreadyInstalled = $this->getTablesInstalled($forceReload = true);
        foreach($tablesAlreadyInstalled as $table)
        {
            Db::query('TRUNCATE "'.$table.'"');
        }
    }

    /**
     * Drop specific tables
     *
     * @param array  $doNotDelete  Names of tables to not delete
     */
    public function dropTables( $doNotDelete = array() )
    {
        $tablesAlreadyInstalled = $this->getTablesInstalled();
        $db = Db::get();

        $doNotDeletePattern = '/('.implode('|',$doNotDelete).')/';

        foreach($tablesAlreadyInstalled as $tableName)
        {
            if( count($doNotDelete) == 0
                || (!in_array($tableName,$doNotDelete)
                    && !preg_match($doNotDeletePattern,$tableName)
                    )
                )
            {
                $db->query('DROP TABLE IF EXISTS "'.$tableName.'"');
            }
        }
    }
}
