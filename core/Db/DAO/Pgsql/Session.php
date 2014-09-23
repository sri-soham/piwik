<?php /** * Piwik - Open source web analytics *
 * @link http://piwik.org
 * @license http://www.gnu.org/licenses/gpl-3.0.html GPL v3 or later
 *
 * @category Piwik
 * @package Piwik
 */
namespace Piwik\Db\DAO\Mysql;

use Piwik\Db\DAO\Base;

/**
 * Session
 *
 * Doesn't add any functionality. This has been created so that the
 * getTablesWithData and restoreDbTables of the IntegrationTestCase
 * have some classes for the session table.
 *
 * @package Piwik
 * @subpackage Piwik_Db
 */
class Session extends \Piwik\Db\DAO\Mysql\Session
{
	public function __construct($db, $table)
    {
        parent::__construct($db, $table);
    }

    public function write($id, $data, $maxLifetime)
    {
        $sql = 'SELECT id, modified, lifetime, data FROM ' . $this->table . ' '
             . 'WHERE id = ?';
        $row = $this->db->fetchRow($sql, array($id));

        if ($row) {
            $sql = 'UPDATE ' . $this->table . ' SET '
                 . ' modified = ?, lifetime = ?, data = ? '
                 . 'WHERE id = ?';
            $this->db->query($sql, array(time(), $maxLifetime, $data, $id));
        }
        else {
            $this->db->insert(
                $this->table,
                array(
                	'id' => $id, 
                	'modified' => time(),
                	'lifetime' => $maxLifetime,
                	'data' => $data,
                )
            );
        }
    }
}
