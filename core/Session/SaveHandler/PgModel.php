<?php
/**
 * Piwik - free/libre analytics platform
 *
 * @link http://piwik.org
 * @license http://www.gnu.org/licenses/gpl-3.0.html GPL v3 or later
 *
 */

namespace Piwik\Session\SaveHandler;

use Piwik\Db\Factory;

class PgModel extends Model {
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
        $Generic = Factory::getGeneric();
        $sql = 'SELECT ' . $this->config['dataColumn'] . ' FROM ' . $this->config['name'] . ' WHERE ' . $this->config['primary'] . ' = ? FOR UPDATE';

        $Generic->beginTransaction();
        $row = $this->db->fetchOne($sql, array($id));
        if ($row) {
            $sql = 'UPDATE ' . $this->config['name'] . ' SET '
                . $this->config['modifiedColumn'] . ' = ?,'
                . $this->config['lifetimeColumn'] . ' = ?,'
                . $this->config['dataColumn'] . ' = ? '
                . ' WHERE ' . $this->config['primary'] . ' = ?';

            $values = array(time(), $maxLifetime, $data, $id);
        }
        else {
            $sql = 'INSERT INTO ' . $this->config['name']
                . ' (' . $this->config['primary'] . ','
                . $this->config['modifiedColumn'] . ','
                . $this->config['lifetimeColumn'] . ','
                . $this->config['dataColumn'] . ')'
                . ' VALUES (?,?,?,?)';
    
            $values = array($id, time(), $maxLifetime, $data);
        }

        $this->db->query($sql, $values);
        $Generic->commit();

        return true;
    }

}

