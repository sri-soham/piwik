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
namespace Piwik\Db\DAO\Pgsql;

use Piwik\Db\Factory;

/**
 * @package Piwik
 * @subpackage Piwik_Db
 */
class Option extends \Piwik\Db\DAO\Mysql\Option
{
    public function __construct($db, $table)
    {
        parent::__construct($db, $table);
    }

    public function addRecord($name, $value, $autoload)
    {
        $generic = Factory::getGeneric($this->db);

        $generic->beginTransaction();
        $sql = 'SELECT * FROM ' . $this->table . ' WHERE option_name = ?';
        $row = $this->db->fetchOne($sql, array($name));

        if ($row) {
            $sql = 'UPDATE ' . $this->table . ' SET option_value = ? '
                 . 'WHERE option_name = ?';
            $bind = array($value, $name);
        }
        else {
            $sql = 'INSERT INTO ' . $this->table . ' (option_name, option_value, autoload) '
                 . 'VALUES (?, ?, ?) ';
            $bind = array($name, $value, $autoload);
        }

        $this->db->query($sql, $bind);

        $generic->commit();
    }

    public function getAllAutoload()
    {
        $sql = 'SELECT option_value, option_name FROM ' . $this->table
             . " WHERE autoload = '1'";
        return $this->db->fetchAll($sql);
    }

    public function fetchAll()
    {
        $sql = 'SELECT * FROM ' . $this->table;
        $rows = $this->db->fetchAll($sql);
        while (list($k, $row) = each($rows)) {
            $rows[$k]['autoload'] = ($row['autoload'] === '1' || $row['autoload'] === 't') ? 't' : 'f';

        }
        reset($rows);

        return $rows;
    }
}
