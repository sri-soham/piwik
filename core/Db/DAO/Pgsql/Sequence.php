<?php
/**
 *  Piwik _ Open source web analytics
 *
 *  @link http://piwik.org
 *  @license http://www.gnu.org/licenses/gpl-3.0.html GPL v3 or later
 *
 *  @category Piwik
 *  @package Piwik
 */
namespace Piwik\Db\DAO\Pgsql;

use Piwik\Db\Factory;

class Sequence extends \Piwik\Db\DAO\Mysql\Sequence {
    /**
     * Get / allocate / reserve a new id for the current sequence. Important: Getting the next id will fail in case
     * no such sequence exists. Make sure to create one if needed, see {@link create()}.
     *
     * @return int
     * @throws Exception
     */
    public function getNextId()
    {
        $Generic = Factory::getGeneric($this->db);
        $Generic->beginTransaction();
        $sql   = 'UPDATE ' . $this->table . ' SET value = value + 1 WHERE name = ?';

        $result   = $this->db->query($sql, array($this->name));
        $rowCount = $result->rowCount();

        if (1 !== $rowCount) {
            $Generic->rollback();
            throw new \Exception("Sequence '" . $this->name . "' not found.");
        }

        $createdId = $this->getCurrentId();
        $Generic->commit();

        return (int) $createdId;
    }
}

