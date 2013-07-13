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

class Piwik_Db_DAO_Pgsql_Segment extends Piwik_Db_DAO_Segment
{ 
    public function add($bind)
    {
        $this->db->insert($this->table, $bind);
        return $this->db->lastInsertId($this->table.'_idsegment');
    }

    public function install()
    {
        $query = 'CREATE TABLE "' . $this->table . '" (
                    idsegment SERIAL4 NOT NULL ,
                    name VARCHAR(255) NOT NULL,
                    definition TEXT NOT NULL,
                    login VARCHAR(100) NOT NULL,
                    enable_all_users SMALLINT NOT NULL default 0,
                    enable_only_idsite INTEGER NULL,
                    auto_archive SMALLINT NOT NULL default 0,
                    ts_created TIMESTAMP NULL,
                    ts_last_edit TIMESTAMP NULL,
                    deleted SMALLINT NOT NULL default 0,
                    PRIMARY KEY (idsegment)
                )';
        try {
            $this->db->query($query);
        } catch (Exception $e) {
            if (!$this->db->isErrNo($e, '1050')) {
                throw $e;
            }
        }
    }
}
