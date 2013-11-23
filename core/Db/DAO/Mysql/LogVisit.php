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

use Piwik\Common;
use Piwik\Config;
use Piwik\Db;
use Piwik\Db\Factory;
use Piwik\Db\DAO\Base;
use Piwik\Segment;

/**
 * @package Piwik
 * @subpackage Piwik_Db
 */

class LogVisit extends Base
{ 
    // Used only with the "recognizeVisitor" function
    // To avoid passing in and out too many parameters.
    protected $recognize = null;

    protected $Generic = null;

    public function __construct($db, $table)
    {
        parent::__construct($db, $table);
    }

    public function addColLocationProvider()
    {
        $sql = 'ALTER IGNORE TABLE ' . $this->table . ' ADD COLUMN '
             . 'location_provider VARCHAR(100) DEFAULT NULL';
        // if the column already exist do not throw error. Could be installed twice...
        try {
            $this->db->exec($sql);
        }
        catch(\Exception $e) {
            if (!$this->db->isErrNo($e, '1060')) {
                throw $e;
            }
        }
    }

    public function removeColLocationProvider()
    {
        $sql = 'ALTER TABLE ' . $this->table . ' DROP COLUMN location_provider';
        $this->db->exec($sql);
    }

    public function getDeleteIdVisitOffset($date, $maxIdVisit, $segmentSize)
    {
        $sql = 'SELECT idvisit '
             . 'FROM ' . $this->table . ' '
             . "WHERE '" . $date ."' > visit_last_action_time "
             . '  AND idvisit <= ? '
             . '  AND idvisit > ? '
             . 'ORDER BY idvisit DESC '
             . 'LIMIT 1';

        return Db::segmentedFetchFirst($sql, $maxIdVisit, 0, $segmentSize);
    }

    public function update($valuesToUpdate, $idsite, $idvisit)
    {
        $Generic = Factory::getGeneric($this->db);
        $valuesToUpdate['idvisitor'] = $Generic->bin2db($valuesToUpdate['idvisitor']);

        $updateParts = $sqlBind = array();
        foreach ($valuesToUpdate AS $name => $value) {
            // Case where bind parameters don't work
            if(strpos($value, $name) !== false) {
                //$name = 'visit_total_events'
                //$value = 'visit_total_events + 1';
                $updateParts[] = " $name = $value ";
            } else {
                $updateParts[] = $name . " = ?";
                $sqlBind[] = $value;
            }
        }
        
        array_push($sqlBind, $idsite, $idvisit);
        $sql = 'UPDATE ' . $this->table . ' SET '
             . implode(', ', $updateParts) . ' '
             . 'WHERE idsite = ? AND idvisit = ?';
        $result = $this->db->query($sql, $sqlBind);

        return array($this->db->rowCount($result),
                     $sql,
                     $sqlBind
                    );
    }

    public function add($visitor_info)
    {
        $fields = implode(', ', array_keys($visitor_info));
        $values = Common::getSqlStringFieldsArray($visitor_info);
        $bind   = array_values($visitor_info);

        $sql = 'INSERT INTO ' . $this->table . '( ' . $fields . ') VALUES (' . $values . ')';

        $this->db->query($sql, $bind);

        return $this->db->lastInsertId();
    }

    /**
     *  recognizeVisitor
     *
     *  Uses tracker db
     *
     *  @param bool $customVariablesSet If custom variables are set in request
     *  @param string $timeLookBack Timestamp from which records will be checked
     *  @param bool $shouldMatchOneFieldOnly
     *  @param bool $matchVisitorId
     *  @param int  $idSite
     *  @param int  $configId
     *  @param int  $idVisitor
     *  @return array
     */
    public function recognizeVisitor($customVariablesSet, $timeLookBack, $timeLookAhead,
                                     $shouldMatchOneFieldOnly, $matchVisitorId,
                                     $idSite, $configId, $idVisitor)
    {
        $this->Generic = Factory::getGeneric($this->db);

        $this->recognize = array();
        $this->recognize['matchVisitorId'] = $matchVisitorId;
        $this->recognize['configId'] = $configId;
        $this->recognize['idVisitor'] = $idVisitor;
        $this->recognize['whereCommon'] = ' visit_last_action_time >= ? AND visit_last_action_time <= ? AND idsite = ? ';
        $this->recognize['bind'] = array($timeLookBack, $timeLookAhead, $idSite);

        $this->recognizeVisitorSelect($customVariablesSet);

        // Two use cases:
        // 1) there is no visitor ID so we try to match only on config_id (heuristics)
        //      Possible causes of no visitor ID: no browser cookie support, direct Tracking API request without visitor ID passed, etc.
        //      We can use config_id heuristics to try find the visitor in the past, there is a risk to assign 
        //      this page view to the wrong visitor, but this is better than creating artificial visits.
        // 2) there is a visitor ID and we trust it (config setting trust_visitors_cookies), so we force to look up this visitor id
        if ($shouldMatchOneFieldOnly) {
            $this->recognizeVisitorOneField();
        }
        /* We have a config_id AND a visitor_id. We match on either of these.
                Why do we also match on config_id?
                we do not tru st the visitor ID only. Indeed, some browsers, or browser addons, 
                cause the visitor id from the 1st party cookie to be different on each page view! 
                It is not acceptable to create a new visit every time such browser does a page view, 
                so we also backup by searching for matching config_id. 
         We use a UNION here so that each sql query uses its own INDEX
        */
        else {
            $this->recognizeVisitorTwoFields();
        }

        $result = $this->db->fetchRow($this->recognize['sql'], $this->recognize['bind']);

        return array($result, $this->recognize['selectCustomVariables']);
    }

    public function getCountByIdvisit($idvisit)
    {
        $sql = 'SELECT COUNT(*) FROM ' . $this->table . ' WHERE idvisit <= ?';
        return (int)$this->db->fetchOne($sql, array($idvisit));
    }

    public function getMaxIdvisit()
    {
        $sql = 'SELECT MAX(idvisit) FROM ' . $this->table;
        return $this->db->fetchOne($sql);
    }

    public function loadLastVisitorDetailsSelect()
    {
        return 'log_visit.*';
    }

    public function loadLastVisitorDetails($subQuery, $sqlLimit, $orderByParent)
    {
        $sql = 'SELECT sub.* FROM ( '
             .   $subQuery['sql'] . $sqlLimit
             . ' ) AS sub '
             . 'GROUP BY sub.idvisit '
             . 'ORDER BY ' . $orderByParent;
        
        return $this->db->fetchAll($sql, $subQuery['bind']);
    }

    public function getCounters($sql, $bind)
    {
        return $this->db->fetchAll($sql, $bind);
    }

    public function updateVisit($idVisit, $uaDetails)
    {
        $q = "UPDATE {$this->table} SET " .
            "config_browser_name = '" . $uaDetails['config_browser_name'] . "' ," .
            "config_browser_version = '" . $uaDetails['config_browser_version'] . "' ," .
            "config_os = '" . $uaDetails['config_os'] . "' ," .
            "config_os_version = '" . $uaDetails['config_os_version'] . "' ," .
            "config_device_type =  " . (isset($uaDetails['config_device_type']) ? "'" . $uaDetails['config_device_type'] . "'" : "NULL") . " ," .
            "config_device_model = " . (isset($uaDetails['config_device_model']) ? "'" . $uaDetails['config_device_model'] . "'" : "NULL") . " ," .
            "config_device_brand = " . (isset($uaDetails['config_device_brand']) ? "'" . $uaDetails['config_device_brand'] . "'" : "NULL") . "
                    WHERE idvisit = " . $idVisit;
        $this->db->query($q);
    }

    public function getAdjacentVisitorId($idSite,
                                         $visitorId,
                                         $orderByDir,
                                         $visitLastActionTimeCondition,
                                         $visitLastActionTime,
                                         $segment)
    {
        $select = "log_visit.idvisitor, MAX(log_visit.visit_last_action_time) as visit_last_action_time";
        $from = "log_visit";
        $where = "log_visit.idsite = ? AND log_visit.idvisitor <> UNHEX(?)";
        $whereBind = array($idSite, $visitorId);
        $orderBy = "MAX(log_visit.visit_last_action_time) $orderByDir";
        $groupBy = "log_visit.idvisitor";

        $segment = new Segment($segment, $idSite);
        $queryInfo = $segment->getSelectQuery($select, $from, $where, $whereBind, $orderBy, $groupBy);

        $sql = "SELECT sub.idvisitor, sub.visit_last_action_time
                  FROM ({$queryInfo['sql']}) as sub
                 WHERE $visitLastActionTimeCondition
                 LIMIT 1";
        $bind = array_merge($queryInfo['bind'], array($visitLastActionTime));

        $visitorId = $this->db->fetchOne($sql, $bind);
        if (!empty($visitorId)) {
            $visitorId = bin2hex($visitorId);
        }

        return $visitorId;
    }

    public function devicesDetectionInstall()
    {
// we catch the exception
        try {
            $q1 = "ALTER TABLE `" . $this->table . "`
                ADD `config_os_version` VARCHAR( 100 ) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL AFTER `config_os` ,
                ADD `config_device_type` VARCHAR( 100 ) NULL DEFAULT NULL AFTER `config_browser_version` ,
                ADD `config_device_brand` VARCHAR( 100 ) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL AFTER `config_device_type` ,
                ADD `config_device_model` VARCHAR( 100 ) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL AFTER `config_device_brand`";
            $this->db->exec($q1);
            // conditionaly add this column
            if (@Config::getInstance()->Debug['store_user_agent_in_visit']) {
                $q2 = "ALTER TABLE `" . $this->table . "`
                ADD `config_debug_ua` VARCHAR( 512 ) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL AFTER `config_device_model`";
                $this->db->exec($q2);
            }
        } catch (\Exception $e) {
            if (!$this->db->isErrNo($e, '1060')) {
                throw $e;
            }
        }
    }

    protected function recognizeVisitorSelect($customVariablesSet)
    {
        if ($customVariablesSet) {
            $this->recognize['selectCustomVariables'] = '';
        }
        else {
            // No custom var were found in the request, so let's copy the previous one in a potential conversion later
            $this->recognize['selectCustomVariables'] = ', 
                custom_var_k1, custom_var_v1,
                custom_var_k2, custom_var_v2,
                custom_var_k3, custom_var_v3,
                custom_var_k4, custom_var_v4,
                custom_var_k5, custom_var_v5 ';
        }

        $idvisitor = $this->Generic->binaryColumn('idvisitor');
        $select = "SELECT   $idvisitor,
                            visit_last_action_time,
                            visit_first_action_time,
                            idvisit,
                            visit_exit_idaction_url,
                            visit_exit_idaction_name,
                            visitor_returning,
                            visitor_days_since_first,
                            visitor_days_since_order,
                            location_country,
                            location_region,
                            location_city,
                            location_latitude,
                            location_longitude,
                            referer_name,
                            referer_keyword,
                            referer_type,
                            visitor_count_visits,
                            visit_goal_buyer
                            {$this->recognize['selectCustomVariables']}
        ";
        $this->recognize['select'] = $select;
        $this->recognize['from'] = ' FROM ' . $this->table . ' ';
    }

    protected function recognizeVisitorOneField()
    {
        $bind = $this->recognize['bind'];
        $where = $this->recognize['whereCommon'];

        if ($this->recognize['matchVisitorId']) {
            $where .= ' AND idvisitor = ? ';
            $bind[] = $this->Generic->bin2db($this->recognize['idVisitor']);
        }
        else {
            $where .= ' AND config_id = ? ';
            $bind[] = $this->Generic->bin2db($this->recognize['configId']);
        }

        $this->recognize['sql'] = $this->recognize['select']
             . $this->recognize['from']
             . 'WHERE ' . $where . ' '
             . 'ORDER BY visit_last_action_time DESC '
             . 'LIMIT 1';
        $this->recognize['bind'] = $bind;
    }

    protected function recognizeVisitorTwoFields()
    {
        $bind = $this->recognize['bind'];
        $whereSameBothQueries = $this->recognize['whereCommon'];
        
        
        $where = ' AND config_id = ?';
        $bind[] = $this->Generic->bin2db($this->recognize['configId']);
        $configSql = $this->recognize['select']." ,
                0 as priority
                {$this->recognize['from']}
                WHERE $whereSameBothQueries $where
                ORDER BY visit_last_action_time DESC
                LIMIT 1
        ";
    
        // will use INDEX index_idsite_idvisitor (idsite, idvisitor)
        $bind = array_merge($bind, $this->recognize['bind']);
        $where = ' AND idvisitor = ? ';
        $bind[] = $this->Generic->bin2db($this->recognize['idVisitor']);
        $visitorSql = "{$this->recognize['select']} ,
                1 as priority
                {$this->recognize['from']} 
                WHERE $whereSameBothQueries $where
                ORDER BY visit_last_action_time DESC
                LIMIT 1
        ";
        
        // We join both queries and favor the one matching the visitor_id if it did match
        $sql = " ( $configSql ) 
                UNION 
                ( $visitorSql ) 
                ORDER BY priority DESC 
                LIMIT 1";
        $this->recognize['sql'] = $sql;
        $this->recognize['bind'] = $bind;
    }
}
