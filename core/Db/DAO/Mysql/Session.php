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
class Session extends Base
{
	public function __construct($db, $table)
    {
        parent::__construct($db, $table);
    }

    public function write($id, $data, $maxLifetime)
    {
    	 $sql = 'INSERT INTO '. $this->table . ' (id, modified, lifetime, data) '
    	 	. 'VALUES (?, ?, ?, ?) '
    	 	. 'ON DUPLICATE KEY UPDATE modified = ?, lifetime = ?, data = ?';
        $this->db->query($sql, array($id, time(), $this->maxLifetime, $data, time(), $this->maxLifetime, $data));
    }
}
