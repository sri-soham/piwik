<?php /** * Piwik - Open source web analytics *
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
class Piwik_Db_DAO_Pgsql_LogConversion extends Piwik_Db_DAO_LogConversion
{
    public function __construct($db, $table)
    {
        parent::__construct($db, $table);
    }

    /**
     *  fetchAll
     *
     *  Fetches all records in the table
     */
    public function fetchAll()
    {
        $generic = Piwik_Db_Factory::getGeneric();
        $generic->checkByteaOutput();
        $sql = 'SELECT *, idvisitor::text AS idvisitor_text FROM ' . $this->table;
        $rows = $this->db->fetchAll($sql);
        while (list($k, $row) = each($rows)) {
            if (!empty($row['idvisitor'])) {
                $rows[$k]['idvisitor'] = $generic->db2bin($row['idvisitor_text']);
            }
            unset($rows[$k]['idvisitor_text']);
        }
        reset($rows);

        return $rows;
    }

    /**
     *  uses tracker db
     */
    protected function recordGoalInsert($goal)
    {
        $Generic = Piwik_Db_Factory::getGeneric($this->db);
        // pg is throwing error when empty values are given for 'FLOAT' columns
        if (empty($goal['revenue'])) unset($goal['revenue']);
        if (empty($goal['revenue_subtotal'])) unset($goal['revenue_subtotal']);
        if (empty($goal['revenue_tax'])) unset($goal['revenue_tax']);
        if (empty($goal['revenue_shipping'])) unset($goal['revenue_shipping']);
        if (empty($goal['revenue_discount'])) unset($goal['revenue_discount']);
        $fields = implode(', ', array_keys($goal));
        $bindFields = Piwik_Common::getSqlStringFieldsArray($goal);
        $goal['idvisitor'] = $Generic->bin2db($goal['idvisitor']);
        $sql = 'INSERT INTO ' . $this->table . '( ' . $fields . ' ) '
             . 'VALUES ( ' . $bindFields . ' ) ';

        $bind = array_values($goal);
        $result = $Generic->insertIgnore($sql, $bind);
        
        return $result;
    }
}
