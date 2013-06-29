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

class Piwik_Db_DAO_LogProfiling extends Piwik_Db_DAO_Base
{ 
    public function __construct($db, $table)
    {
        parent::__construct($db, $table);
    }

    public function getAll()
    {
        $sql = 'SELECT * FROM ' . $this->table;
        return $this->db->fetchAll($sql);
    }

    /**
     *  recordProfiling
     *
     *  Makes an entry in the table for the query if query does not exist.
     *  If the query exists, the row will be update.
     *  The query column has a unique key.
     *  Uses tracker db
     *
     *  @param string $query
     *  @param int    $count
     *  @param string $time
     *  @return void
     */
    public function recordProfiling($query, $count, $time)
    {
        $sql = 'INSERT INTO ' . $this->table . '(query, count, sum_time_ms) '
             . "VALUES (?, $count, $time) "
             . 'ON DUPLICATE KEY UPDATE '
             . "  count = count + $count "
             . ", sum_time_ms = sum_time_ms + $time";
        $this->db->query($sql, array($query));
    }
}
