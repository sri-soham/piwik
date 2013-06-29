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

class Piwik_Db_DAO_Pgsql_LogConversionItem extends Piwik_Db_DAO_LogConversionItem
{ 
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
            $rows[$k]['deleted'] = ($row['deleted'] === '1' ||  $row['deleted'] === 't') ? 't' : 'f';

        }
        reset($rows);

        return $rows;
    }
}
