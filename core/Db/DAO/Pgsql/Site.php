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

class Piwik_Db_DAO_Pgsql_Site extends Piwik_Db_DAO_Site
{ 
    public function __construct($db, $table)
    {
        parent::__construct($db, $table);
    }

    public function addRecord($name,
                              $url,
                              $ips,
                              $params,
                              $user_agents,
                              $keep_url_fragment,
                              $tz,
                              $currency,
                              $ecommerce,
                              $search,
                              $search_keyword_parameters,
                              $search_category_parameters,
                              $ts,
                              $group)
    {
        $bind = array();
        $bind['name']     = $name;
        $bind['main_url'] = $url;
        $bind['excluded_ips'] = $ips;
        $bind['excluded_parameters'] = $params;
        $bind['excluded_user_agents'] = $user_agents;
        $bind['keep_url_fragment'] = $keep_url_fragment;
        $bind['timezone']   = $tz;
        $bind['currency']   = $currency;
        $bind['ecommerce']  = $ecommerce;
        $bind['sitesearch'] = $search;
        $bind['sitesearch_keyword_parameters'] = $search_keyword_parameters;
        $bind['sitesearch_category_parameters'] = $search_category_parameters;
        $bind['ts_created'] = $ts;
        $bind['group'] = $group;

        $this->db->insert($this->table, $bind);

        return $this->db->lastInsertId($this->table.'_idsite');
    }

    public function addColFeedburnername()
    {
        $sql = 'ALTER TABLE ' . $this->table . ' ADD COLUMN feedburner_name VARCHAR(100) DEFAULT NULL';
        try {
            $this->db->exec($sql);
        } catch (Exception $e) {
            // postgresql code error 42701: duplicate_column
            // if there is another error we throw the exception, otherwise it is OK as we are simply reinstalling the plugin
            if (!$this->db->isErrNo($e, '42701')) {
                throw $e;
            }
        }
    }
}
