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
namespace Piwik\Db\DAO;

use Piwik\Common;
use Piwik\Db;
use Piwik\Tracker\GoalManager;

/**
 * @package Piwik
 * @subpackage Piwik_Db
 */

abstract class Base
{
    protected $db;
    protected $table;

    public function __construct($db, $table)
    {
        $this->db = $db;
        $this->table = Common::prefixTable($table);
    }

    public function setTable($table)
    {
        $table = Common::unprefixTable($table);
        $this->table = Common::prefixTable($table);
    }

    public function getSqlRevenue($field)
    {
        return "ROUND(".$field.",".GoalManager::REVENUE_PRECISION.")";
    }

    public function getCount()
    {
        $sql = 'SELECT COUNT(*) FROM ' . $this->table;
        return $this->db->fetchOne($sql);
    }

    public function getDB()
    {
        return $this->db;
    }

    // Used primarily for the test case setup
    public function fetchAll()
    {
        return $this->db->fetchAll('SELECT * FROM ' . $this->table);
    }

    // Used primarily for the test case setup
    public function insertAll($rows)
    {
        $generic = Factory::getGeneric();
        $rowsSql = array();
        foreach ($rows as $row) {
            $values = array();
            foreach ($row as $name => $value) {
                if (is_null($value)) {
                    $values[] = 'NULL';
                }
                else if (is_numeric($value)) {
                    $values[] = $value;
                }
                else if (!ctype_print($value)) {
                    $values[] = $generic->bin2dbRawInsert($value);
                }
                else if (is_bool($value)) {
                    $values[] = $value ? '1' : '0';
                }
                else {
                    $values[] = "'$value'";
                }
            }
            
            $rowsSql[] = "(".implode(',', $values).")";
        }
        $sql = 'INSERT INTO ' . $this->table . ' VALUES ' . implode(',', $rowsSql);
        $this->db->query($sql);
    }
}
