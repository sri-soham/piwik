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

/**
 * @package Piwik
 * @subpackage Piwik_Db
 */

class Site extends \Piwik\Db\DAO\Mysql\Site
{ 
    public function __construct($db, $table)
    {
        parent::__construct($db, $table);
    }

    public function addRecord($bind)
    {
        $this->db->insert($this->table, $bind);
        return $this->db->lastInsertId($this->table.'_idsite');
    }

    public function addColFeedburnername()
    {
        $sql = 'ALTER TABLE ' . $this->table . ' ADD COLUMN feedburner_name VARCHAR(100) DEFAULT NULL';
        try {
            $this->db->exec($sql);
        } catch (\Exception $e) {
            // postgresql code error 42701: duplicate_column
            // if there is another error we throw the exception, otherwise it is OK as we are simply reinstalling the plugin
            if (!$this->db->isErrNo($e, '42701')) {
                throw $e;
            }
        }
    }

    // Mysql does case insensitive comparison for "LIKE" conditions
    // Pgsql does case sensitive comparison, so using ILIKE instead of LIKE
    protected function likeKeyword()
    {
        return 'ILIKE';
    }
}
