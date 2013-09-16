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
class Goal extends Base
{
    public function __construct($db, $table)
    {
        parent::__construct($db, $table);
    }

    public function getAllForIdsites($idsite_array)
    {
        $sql = 'SELECT * FROM ' . $this->table . ' '
             . 'WHERE idsite IN (' . implode(', ', $idsite_array) . ' ) '
             . '  AND deleted = 0';
        return $this->db->fetchAll($sql);
    }

    public function getIdgoalForIdsite($idsite)
    {
        $sql = 'SELECT MAX(idgoal) + 1 FROM ' . $this->table . ' '
             . 'WHERE idsite = ?';
        return $this->db->fetchOne($sql, $idsite);
    }

    public function addRecord($site, $goal, $name, $match_attribute, $pattern, $pattern_type, $case_sensitive, $allow_multiple, $revenue, $deleted)
    {
        $values = array();
        $values['idsite'] = $site;
        $values['idgoal'] = $goal;
        $values['name'] = $name;
        $values['match_attribute'] = $match_attribute;
        $values['pattern'] = $pattern;
        $values['pattern_type'] = $pattern_type;
        $values['case_sensitive'] = $case_sensitive;
        $values['allow_multiple'] = $allow_multiple;
        $values['revenue'] = $revenue;
        $values['deleted'] = $deleted;

        $this->db->insert($this->table, $values);
    }

    public function update($name, $match_attribute, $pattern, $pattern_type, $case_sensitive, $allow_multiple, $revenue, $idsite, $idgoal)
    {
        $this->db->update($this->table,
            array('name' => $name,
                  'match_attribute' => $match_attribute,
                  'pattern' => $pattern,
                  'pattern_type' => $pattern_type,
                  'case_sensitive' => $case_sensitive,
                  'allow_multiple' => $allow_multiple,
                  'revenue' => $revenue
            ),
            "idsite = '$idsite' AND idgoal = '$idgoal'"
        );
    }

    public function markAsDeleted($idsite, $idgoal)
    {
        $sql = 'UPDATE ' . $this->table . ' '
             . ' SET deleted = 1 '
             . 'WHERE idsite = ? AND idgoal = ?';
        $this->db->query($sql, array($idsite, $idgoal));
    }

    public function deleteByIdsite($idsite)
    {
        $sql = 'DELETE FROM ' . $this->table . ' '
             . 'WHERE idsite = ?';
        $this->db->query($sql, $idsite);
    }

    public function getMaxIdgoal()
    {
        $sql = 'SELECT MAX(idgoal) FROM ' . $this->table;
        return $this->db->fetchOne($sql);
    }
}
