<?php
/**
 * Piwik - free/libre analytics platform
 *
 * @link http://piwik.org
 * @license http://www.gnu.org/licenses/gpl-3.0.html GPL v3 or later
 *
 */

namespace Piwik\Session\SaveHandler;

use Piwik\Db;

class Model implements \Piwik\Db\FactoryCreated {
    protected $db;
    protected $config;

    public function __construct() {
        $this->db = Db::get();
    }

    public function setConfig($config) {
        $this->config = $config;
    }

    /**
     * Read session data
     *
     * @param string $id
     * @return string
     */
    public function read($id)
    {
        $sql = 'SELECT ' . $this->config['dataColumn'] . ' FROM ' . $this->config['name']
            . ' WHERE ' . $this->config['primary'] . ' = ?'
            . ' AND ' . $this->config['modifiedColumn'] . ' + ' . $this->config['lifetimeColumn'] . ' >= ?';

        $result = $this->db->fetchOne($sql, array($id, time()));
        if (!$result) {
            $result = '';
        }

        return $result;
    }

    /**
     * Write Session - commit data to resource
     *
     * @param string $id
     * @param mixed $data
     * @param integer $maxLifetime
     * @return boolean
     */
    public function write($id, $data, $maxLifetime)
    {
        $sql = 'INSERT INTO ' . $this->config['name']
            . ' (' . $this->config['primary'] . ','
            . $this->config['modifiedColumn'] . ','
            . $this->config['lifetimeColumn'] . ','
            . $this->config['dataColumn'] . ')'
            . ' VALUES (?,?,?,?)'
            . ' ON DUPLICATE KEY UPDATE '
            . $this->config['modifiedColumn'] . ' = ?,'
            . $this->config['lifetimeColumn'] . ' = ?,'
            . $this->config['dataColumn'] . ' = ?';

        $this->db->query($sql, array($id, time(), $maxLifetime, $data, time(), $maxLifetime, $data));

        return true;
    }

    /**
     * Destroy Session - remove data from resource for
     * given session id
     *
     * @param string $id
     * @return boolean
     */
    public function destroy($id)
    {
        $sql = 'DELETE FROM ' . $this->config['name'] . ' WHERE ' . $this->config['primary'] . ' = ?';

        $this->db->query($sql, array($id));

        return true;
    }

    /**
     * Garbage Collection - remove old session data older
     * than $maxlifetime (in seconds)
     *
     * @param int $maxlifetime timestamp in seconds
     * @return bool  always true
     */
    public function gc($maxlifetime)
    {
        $sql = 'DELETE FROM ' . $this->config['name']
            . ' WHERE ' . $this->config['modifiedColumn'] . ' + ' . $this->config['lifetimeColumn'] . ' < ?';

        $this->db->query($sql, array(time()));

        return true;
    }
}
