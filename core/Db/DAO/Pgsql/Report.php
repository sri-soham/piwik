<?php
/**
 * Piwik - Open source web analytics
 $*
 * @link http://piwik.org
 * @license http://www.gnu.org/licenses/gpl-3.0.html GPL v3 or later
 *
 * @category Piwik
 * @package Piwik
 */
namespace Piwik\Db\DAO\Pgsql;

use Piwik\Common;

/**
 * @package Piwik
 * @subpackage Piwik_Db
 */

class Report extends \Piwik\Db\DAO\Mysql\Report
{
    public function __construct($db, $table)
    {
        parent::__construct($db, $table);
    }

    public function insert($idreport, $idsite, $login, $description, $idsegment, $period, $hour,
                           $type, $format, $parameters, $reports, $ts_created, $deleted)
    {
        $deleted = ($deleted) ? '1' : '0';
        $this->db->insert($this->table,
            array(
                'idreport'    => $idreport,
                'idsite'      => $idsite,
                'login'       => $login,
                'description' => $description,
                'idsegment'   => $idsegment,
                'period'      => $period,
                'hour'        => $hour,
                'type'        => $type,
                'format'      => $format,
                'parameters'  => $parameters,
                'reports'     => $reports,
                'ts_created'  => $ts_created,
                'deleted'     => $deleted
            )
        );
    }

    public function updateByIdreport($values, $idreport)
    {
        if (array_key_exists('deleted', $values)) {
            $values['deleted'] = $values['deleted'] ? '1' : '0';
        }
        // remove non-digits
        $idreport = preg_replace("/[^\d]/", "", $idreport);
        $this->db->update($this->table, $values, "idreport = $idreport");
    }

    public function getAllActive($idSite, $period, $idReport, $idSegment, $ifSuperUserReturnOnlySuperUserReports)
    {
        list($where, $params) = $this->varsGetAllActive($idSite, $period, $idReport, $idSegment, $ifSuperUserReturnOnlySuperUserReports);
        if (empty($where)) {
            $where = '';
        }
        else {
            $where = ' AND ' . implode(' AND ', $where) . ' ';
        }
        $sql = 'SELECT report.* FROM ' . $this->table . ' AS report '
             . 'INNER JOIN ' . Common::prefixTable('site') . ' '
             . '    USING(idsite) '
             . 'WHERE deleted = \'0\' ' . $where;

        $rows = $this->db->fetchAll($sql, $params);
        return $rows;
    }

    public function createTable()
    {
        $sql = 'CREATE TABLE IF NOT EXISTS '.$this->table . '(
                    idreport SERIAL4 NOT NULL,
                    idsite INTEGER NOT NULL,
                    login VARCHAR(100) NOT NULL,
                    description VARCHAR(255) NOT NULL,
                    idsegment INTEGER,
                    period VARCHAR(10) NOT NULL,
                    hour SMALLINT NOT NULL DEFAULT 0,
                    type VARCHAR(10) NOT NULL,
                    format VARCHAR(10) NOT NULL,
                    reports TEXT NOT NULL,
                    parameters TEXT NULL,
                    ts_created TIMESTAMP NULL,
                    ts_last_sent TIMESTAMP NULL,
                    deleted BOOLEAN DEFAULT \'0\',
                    PRIMARY KEY (idreport)
                )';
        $this->db->exec($sql);
    }
}
