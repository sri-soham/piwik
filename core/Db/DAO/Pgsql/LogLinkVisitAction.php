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
class LogLinkVisitAction extends \Piwik\Db\DAO\Mysql\LogLinkVisitAction
{
    public function __construct($db, $table)
    {
        parent::__construct($db, $table);
    }

    public function record($idvisit, $idsite, $idvisitor, $server_time,
                        $url, $name, $ref_url, $ref_name, $time_spent,
                        $custom_value, $custom_variables, $actionIdsCached
                        )
    {
        list($sql, $bind) = $this->paramsRecord(
            $idvisit, $idsite, $idvisitor, $server_time,
            $url, $name, $ref_url, $ref_name, $time_spent,
            $custom_value, $custom_variables, $actionIdsCached
        );

        $this->db->query($sql, $bind);
        $id = $this->db->lastInsertId($this->table.'_idlink_va');
        $insert['idlink_va'] = $id;

        return $insert;
    }

    public function fetchAll()
    {
        $generic = Factory::getGeneric();
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
} 
