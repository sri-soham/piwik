<?php
/**
 * Piwik - Open source web nalytics
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

class LogProfiling extends \Piwik\Db\DAO\Mysql\LogProfiling
{ 
    public function __construct($db, $table)
    {
        parent::__construct($db, $table);
    }

    /**
     *  recordProfiling
     *
     *  Makes an entry in the table for the query if query does not exist.
     *  If the query exists, the row will be update.
     *  The query column has a unique key.
     *  Uses tracker db
     
     *  @param string $query
     *  @param int    $count
     *  @param string $time
     *  @return void
     */
    public function recordProfiling($query, $count, $time)
    {
        $generic = Factory::getGeneric($this->db);
        $generic->beginTransaction();
        $sql = 'SELECT query, count, sum_time_ms '
             . 'FROM ' . $this->table . ' '
             . 'WHERE query = ? FOR UPDATE';
        $row = $this->db->fetchOne($sql, array($query));
        if ($row) {
            $sql = 'UPDATE ' . $this->table . ' SET '
                 . " count = count + $count, sum_time_ms = sum_time_msg + $time "
                 . 'WHERE query = ?';
            $this->db->query($sql, array($query));
        }
        else {
            $sql = 'INSERT INTO ' . $this->table . '(query, count, sum_time_ms) '
                 . "VALUES (?, ?, ?)";
            $this->db->query($sql, array($query, $count, $time));
        }

        $generic->commit();
    }
}
