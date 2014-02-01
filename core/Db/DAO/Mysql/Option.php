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

use Piwik\Db\DAO\Base;

/**
 * @package Piwik
 * @subpackage Piwik_Db
 */
class Option extends Base
{
    public function __construct($db, $table)
    {
        parent::__construct($db, $table);
        $this->table = $db->quoteIdentifier($this->table);
    }

    public function getValueByName($name)
    {
        $sql = 'SELECT option_value FROM ' . $this->table . ' WHERE option_name = ?';
        return $this->db->fetchOne($sql, array($name));
    }

    public function addRecord($name, $value, $autoload)
    {
        $sql = 'INSERT INTO ' . $this->table . ' (option_name, option_value, autoload) '
             . 'VALUES (?, ?, ?) '
             . 'ON DUPLICATE KEY UPDATE option_value = ?';
        $this->db->query($sql, array($name, $value, $autoload, $value));
    }

    public function delete($name, $value = null)
    {
        $sql = 'DELETE FROM ' . $this->table . ' WHERE option_name = ?';
        $params = array();
        $params[] = $name;
        if (isset($value)) {
            $sql .= ' AND option_value = ? ';
            $params[] = $value;
        }

        $this->db->query($sql, $params);
    }

    public function deleteLike($name, $value = null)
    {
        $sql = 'DELETE FROM ' . $this->table . ' WHERE option_name LIKE ? ';
        $params = array();
        $params[] = $name;
        if (isset($value)) {
            $sql .= ' AND option_value = ?';
            $params[] = $value;
        }

        $this->db->query($sql, $params);
    }

    public function getAllAutoload()
    {
        $sql = 'SELECT option_value, option_name FROM ' . $this->table
             . " WHERE autoload = '1'";
        return $this->db->fetchAll($sql);
    }

    public function sqlUpdate($name, $value)
    {
        $sql = 'UPDATE ' . $this->table . ' SET '
             . "  option_value = '$value' "
             . "WHERE option_name = '$name' ";
        return $sql;
    }

    public function deleteByOptionNames($optionNames) {
        $sql = 'DELETE FROM '. $this->table
             . " WHERE option_name In ('" . implode("','", $optionNames) . "')";
        $this->db->query($sql);
    }
}
